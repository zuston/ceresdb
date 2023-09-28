// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! dist sql query physical plans

use std::{
    any::Any,
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef as ArrowSchemaRef, record_batch::RecordBatch};
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        aggregates::{AggregateExec, AggregateMode},
        coalesce_batches::CoalesceBatchesExec,
        coalesce_partitions::CoalescePartitionsExec,
        displayable,
        filter::FilterExec,
        projection::ProjectionExec,
        repartition::RepartitionExec,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream as DfSendableRecordBatchStream, Statistics,
    },
};
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use table_engine::{remote::model::TableIdentifier, table::ReadRequest};

use crate::dist_sql_query::RemotePhysicalPlanExecutor;

/// Placeholder of partitioned table's scan plan
/// It is inexecutable actually and just for carrying the necessary information
/// of building remote execution plans for sub tables.
// TODO: can we skip this and generate `ResolvedPartitionedScan` directly?
#[derive(Debug)]
pub struct UnresolvedPartitionedScan {
    pub sub_tables: Vec<TableIdentifier>,
    pub read_request: ReadRequest,
}

impl ExecutionPlan for UnresolvedPartitionedScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.read_request
            .projected_schema
            .to_projected_arrow_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.sub_tables.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "UnresolvedPartitionedScan should not have children".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        Err(DataFusionError::Internal(
            "UnresolvedPartitionedScan can not be executed".to_string(),
        ))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for UnresolvedPartitionedScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnresolvedPartitionedScan: sub_tables={:?}, read_request:{:?}, partition_count={}",
            self.sub_tables,
            self.read_request,
            self.output_partitioning().partition_count(),
        )
    }
}

/// The executable scan plan of the partitioned table
/// It includes remote execution plans of sub tables, and will send them to
/// related nodes to execute.
#[derive(Debug)]
pub struct ResolvedPartitionedScan {
    pub remote_exec_ctx: Arc<RemoteExecContext>,
    pub pushing_down: bool,
}

impl ResolvedPartitionedScan {
    pub fn try_to_push_down_more(
        &self,
        cur_node: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Can not push more...
        if !self.pushing_down {
            return cur_node.with_new_children(vec![self.push_down_finished()]);
        }

        // Push down more, and when occur the terminated push down able node, we need to
        // set `can_push_down_more` false.
        let push_down_able_opt = PushDownAble::try_new(cur_node.clone());
        let (node, can_push_down_more) = match push_down_able_opt {
            Some(PushDownAble::Continue(node)) => (node, true),
            Some(PushDownAble::Terminated(node)) => (node, false),
            None => {
                let partitioned_scan = self.push_down_finished();
                return cur_node.with_new_children(vec![partitioned_scan]);
            }
        };

        let new_plans = self
            .remote_exec_ctx
            .plans
            .iter()
            .map(|(table, plan)| {
                node.clone()
                    .with_new_children(vec![plan.clone()])
                    .map(|extended_plan| (table.clone(), extended_plan))
            })
            .collect::<DfResult<Vec<_>>>()?;

        let remote_exec_ctx = Arc::new(RemoteExecContext {
            executor: self.remote_exec_ctx.executor.clone(),
            plans: new_plans,
        });
        let plan = ResolvedPartitionedScan {
            remote_exec_ctx,
            pushing_down: can_push_down_more,
        };

        Ok(Arc::new(plan))
    }

    pub fn new(
        remote_executor: Arc<dyn RemotePhysicalPlanExecutor>,
        remote_exec_plans: Vec<(TableIdentifier, Arc<dyn ExecutionPlan>)>,
    ) -> Self {
        let remote_exec_ctx = Arc::new(RemoteExecContext {
            executor: remote_executor,
            plans: remote_exec_plans,
        });

        Self {
            remote_exec_ctx,
            pushing_down: true,
        }
    }

    pub fn push_down_finished(&self) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            remote_exec_ctx: self.remote_exec_ctx.clone(),
            pushing_down: false,
        })
    }
}

#[derive(Debug)]
pub struct RemoteExecContext {
    executor: Arc<dyn RemotePhysicalPlanExecutor>,
    plans: Vec<(TableIdentifier, Arc<dyn ExecutionPlan>)>,
}

pub enum PushDownAble {
    Continue(Arc<dyn ExecutionPlan>),
    Terminated(Arc<dyn ExecutionPlan>),
}

impl PushDownAble {
    pub fn try_new(plan: Arc<dyn ExecutionPlan>) -> Option<Self> {
        if let Some(aggr) = plan.as_any().downcast_ref::<AggregateExec>() {
            if *aggr.mode() == AggregateMode::Partial {
                Some(Self::Terminated(plan))
            } else {
                None
            }
        } else if let Some(_) = plan.as_any().downcast_ref::<FilterExec>() {
            Some(Self::Continue(plan))
        } else if let Some(_) = plan.as_any().downcast_ref::<ProjectionExec>() {
            Some(Self::Continue(plan))
        } else if let Some(_) = plan.as_any().downcast_ref::<RepartitionExec>() {
            Some(Self::Continue(plan))
        } else if let Some(_) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
            Some(Self::Continue(plan))
        } else if let Some(_) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
            Some(Self::Continue(plan))
        } else {
            None
        }
    }
}

impl ExecutionPlan for ResolvedPartitionedScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.remote_exec_ctx
            .plans
            .first()
            .expect("remote_exec_plans should not be empty")
            .1
            .schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.remote_exec_ctx.plans.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.remote_exec_ctx
            .plans
            .iter()
            .map(|(_, plan)| plan.clone())
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "UnresolvedPartitionedScan can't be built directly from new children".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        if self.pushing_down {
            return Err(DataFusionError::Internal(format!(
                "partitioned scan can't be executed before pushing down finished, plan:{}",
                displayable(self).indent(true).to_string()
            )));
        }

        let (sub_table, plan) = &self.remote_exec_ctx.plans[partition];

        // Send plan for remote execution.
        let stream_future =
            self.remote_exec_ctx
                .executor
                .execute(sub_table.clone(), &context, plan.clone())?;
        let record_stream = PartitionedScanStream::new(stream_future, plan.schema());

        Ok(Box::pin(record_stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// Partitioned scan stream
pub(crate) struct PartitionedScanStream {
    /// Future to init the stream
    stream_future: BoxFuture<'static, DfResult<DfSendableRecordBatchStream>>,

    /// Stream to poll the records
    stream_state: StreamState,

    /// Record schema
    arrow_record_schema: ArrowSchemaRef,
}

impl PartitionedScanStream {
    /// Create an empty RecordBatchStream
    pub fn new(
        stream_future: BoxFuture<'static, DfResult<DfSendableRecordBatchStream>>,
        arrow_record_schema: ArrowSchemaRef,
    ) -> Self {
        Self {
            stream_future,
            stream_state: StreamState::Initializing,
            arrow_record_schema,
        }
    }
}

impl RecordBatchStream for PartitionedScanStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_record_schema.clone()
    }
}

impl Stream for PartitionedScanStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            let stream_state = &mut this.stream_state;
            match stream_state {
                StreamState::Initializing => {
                    let poll_res = this.stream_future.poll_unpin(cx);
                    match poll_res {
                        Poll::Ready(Ok(stream)) => {
                            *stream_state = StreamState::Polling(stream);
                        }
                        Poll::Ready(Err(e)) => {
                            *stream_state = StreamState::InitializeFailed;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                StreamState::InitializeFailed => return Poll::Ready(None),
                StreamState::Polling(stream) => return stream.poll_next_unpin(cx),
            }
        }
    }
}

/// Stream state
/// Before polling record batch from it, we must initializing the record batch
/// stream first. The process of state changing is like:
///
/// ```plaintext
///     ┌────────────┐                                        
///     │Initializing│                                        
///     └──────┬─────┘                                        
///   _________▽_________     ┌──────────────────────────────┐
///  ╱                   ╲    │Polling(we just return the    │
/// ╱ Success to init the ╲___│inner stream's polling result)│
/// ╲ record batch stream ╱yes└──────────────────────────────┘
///  ╲___________________╱                                    
///           │no                                            
///  ┌────────▽───────┐                                      
///  │InitializeFailed│                                      
///  └────────────────┘                                      
/// ```
pub(crate) enum StreamState {
    Initializing,
    InitializeFailed,
    Polling(DfSendableRecordBatchStream),
}

impl DisplayAs for ResolvedPartitionedScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ResolvedPartitionedScan: pushing_down:{}, partition_count:{}",
                    self.pushing_down,
                    self.remote_exec_ctx.plans.len()
                )
            }
        }
    }
}

/// Placeholder of sub table's scan plan
/// It is inexecutable actually and just for carrying the necessary information
/// of building the executable scan plan.
#[derive(Debug, Clone)]
pub struct UnresolvedSubTableScan {
    pub table: TableIdentifier,
    pub read_request: ReadRequest,
}

impl ExecutionPlan for UnresolvedSubTableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.read_request
            .projected_schema
            .to_projected_arrow_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.read_request.opts.read_parallelism)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "UnresolvedSubTableScan should not have children".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        Err(DataFusionError::Internal(
            "UnresolvedSubTableScan can not be executed".to_string(),
        ))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for UnresolvedSubTableScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnresolvedSubTableScan: table:{:?}, request:{:?}, partition_count:{}",
            self.table,
            self.read_request,
            self.output_partitioning().partition_count(),
        )
    }
}

impl TryFrom<ceresdbproto::remote_engine::UnresolvedSubScan> for UnresolvedSubTableScan {
    type Error = DataFusionError;

    fn try_from(
        value: ceresdbproto::remote_engine::UnresolvedSubScan,
    ) -> Result<Self, Self::Error> {
        let table_ident: TableIdentifier = value
            .table
            .ok_or(DataFusionError::Internal(
                "table ident not found".to_string(),
            ))?
            .into();
        let read_request: ReadRequest = value
            .read_request
            .ok_or(DataFusionError::Internal(
                "read request not found".to_string(),
            ))?
            .try_into()
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to decode read request, err:{e}"))
            })?;

        Ok(Self {
            table: table_ident,
            read_request,
        })
    }
}

impl TryFrom<UnresolvedSubTableScan> for ceresdbproto::remote_engine::UnresolvedSubScan {
    type Error = DataFusionError;

    fn try_from(value: UnresolvedSubTableScan) -> Result<Self, Self::Error> {
        let table_ident: ceresdbproto::remote_engine::TableIdentifier = value.table.into();
        let read_request: ceresdbproto::remote_engine::TableReadRequest =
            value.read_request.try_into().map_err(|e| {
                DataFusionError::Internal(format!("failed to encode read request, err:{e}"))
            })?;

        Ok(Self {
            table: Some(table_ident),
            read_request: Some(read_request),
        })
    }
}

#[cfg(test)]
mod test {
    use datafusion::error::DataFusionError;
    use futures::StreamExt;

    use crate::dist_sql_query::{
        physical_plan::PartitionedScanStream,
        test_util::{MockPartitionedScanStreamBuilder, PartitionedScanStreamCase},
    };

    #[tokio::test]
    async fn test_stream_poll_success() {
        let builder = MockPartitionedScanStreamBuilder::new(PartitionedScanStreamCase::Success);
        let mut stream = builder.build();
        let result_opt = stream.next().await;
        assert!(result_opt.is_none());
    }

    #[tokio::test]
    async fn test_stream_init_failed() {
        let builder =
            MockPartitionedScanStreamBuilder::new(PartitionedScanStreamCase::InitializeFailed);
        let stream = builder.build();
        test_stream_failed_state(stream, "failed to init").await
    }

    #[tokio::test]
    async fn test_stream_poll_failed() {
        let builder = MockPartitionedScanStreamBuilder::new(PartitionedScanStreamCase::PollFailed);
        let stream = builder.build();
        test_stream_failed_state(stream, "failed to poll").await
    }

    async fn test_stream_failed_state(mut stream: PartitionedScanStream, failed_msg: &str) {
        // Error happened, check error message.
        let result_opt = stream.next().await;
        assert!(result_opt.is_some());
        let result = result_opt.unwrap();
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DataFusionError::Internal(msg) => {
                assert!(msg.contains(failed_msg))
            }
            other => panic!("unexpected error:{other}"),
        }

        // Should return `None` in next poll.
        let result_opt = stream.next().await;
        assert!(result_opt.is_none());
    }
}
