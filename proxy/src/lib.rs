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

//! The proxy module provides features such as forwarding and authentication,
//! adapts to different protocols.

#![feature(trait_alias)]

pub mod context;
pub mod dedup_requests;
pub mod error;
mod error_util;
pub mod forward;
mod grpc;
pub mod handlers;
pub mod hotspot;
mod hotspot_lru;
pub mod http;
pub mod influxdb;
pub mod instance;
pub mod limiter;
mod metrics;
pub mod opentsdb;
mod query_log;
mod read;
pub mod schema_config_provider;
mod util;
mod write;

pub const FORWARDED_FROM: &str = "forwarded-from";

use std::{
    ops::Bound,
    sync::Arc,
    time::{Duration, Instant},
};

use ::http::StatusCode;
use catalog::{
    schema::{
        CreateOptions, CreateTableRequest, DropOptions, DropTableRequest, NameRef, SchemaRef,
    },
    CatalogRef,
};
use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, PrometheusRemoteQueryRequest,
    PrometheusRemoteQueryResponse, Route,
};
use common_types::{request_id::RequestId, table::DEFAULT_SHARD_ID, ENABLE_TTL, TTL};
use datafusion::{
    prelude::{Column, Expr},
    scalar::ScalarValue,
};
use futures::FutureExt;
use generic_error::BoxError;
use influxql_query::logical_optimizer::range_predicate::find_time_range;
use interpreters::{
    context::Context as InterpreterContext,
    factory::Factory,
    interpreter::{InterpreterPtr, Output},
};
use log::{error, info, warn};
use query_frontend::plan::{Plan, QueryPlan};
use router::{endpoint::Endpoint, RouteRequest, Router};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{CreateTableParams, EngineRuntimes, TableState},
    partition::PartitionInfo,
    remote::model::{GetTableInfoRequest, TableIdentifier, TableInfo},
    table::{TableId, TableRef},
    PARTITION_TABLE_ENGINE_TYPE,
};
use time_ext::{current_time_millis, parse_duration};
use tokio::sync::mpsc::Sender;
use tonic::{transport::Channel, IntoRequest};

use crate::{
    dedup_requests::RequestNotifiers,
    error::{ErrNoCause, ErrWithCause, Error, Internal, Result},
    forward::{ForwardRequest, ForwardResult, Forwarder, ForwarderRef},
    hotspot::HotspotRecorder,
    instance::InstanceRef,
    metrics::{DURATION_SINCE_QUERY_START_TIME, QUERY_TIME_RANGE},
    read::SqlResponse,
    schema_config_provider::SchemaConfigProviderRef,
};

// Because the clock may have errors, choose 1 hour as the error buffer
const QUERY_EXPIRED_BUFFER: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SubTableAccessPerm {
    pub enable_http: bool,
    pub enable_others: bool,
}

impl Default for SubTableAccessPerm {
    fn default() -> Self {
        Self {
            enable_http: true,
            enable_others: true,
        }
    }
}

pub struct Proxy {
    router: Arc<dyn Router + Send + Sync>,
    forwarder: ForwarderRef,
    instance: InstanceRef,
    resp_compress_min_length: usize,
    auto_create_table: bool,
    schema_config_provider: SchemaConfigProviderRef,
    hotspot_recorder: Arc<HotspotRecorder>,
    engine_runtimes: Arc<EngineRuntimes>,
    cluster_with_meta: bool,
    sub_table_access_perm: SubTableAccessPerm,
    request_notifiers: Option<Arc<RequestNotifiers<String, Sender<Result<SqlResponse>>>>>,
}

impl Proxy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        router: Arc<dyn Router + Send + Sync>,
        instance: InstanceRef,
        forward_config: forward::Config,
        local_endpoint: Endpoint,
        resp_compress_min_length: usize,
        auto_create_table: bool,
        schema_config_provider: SchemaConfigProviderRef,
        hotspot_recorder: Arc<HotspotRecorder>,
        engine_runtimes: Arc<EngineRuntimes>,
        cluster_with_meta: bool,
        sub_table_access_perm: SubTableAccessPerm,
        request_notifiers: Option<Arc<RequestNotifiers<String, Sender<Result<SqlResponse>>>>>,
    ) -> Self {
        let forwarder = Arc::new(Forwarder::new(
            forward_config,
            router.clone(),
            local_endpoint,
        ));

        Self {
            router,
            instance,
            forwarder,
            resp_compress_min_length,
            auto_create_table,
            schema_config_provider,
            hotspot_recorder,
            engine_runtimes,
            cluster_with_meta,
            sub_table_access_perm,
            request_notifiers,
        }
    }

    pub fn instance(&self) -> InstanceRef {
        self.instance.clone()
    }

    fn default_catalog_name(&self) -> NameRef {
        self.instance.catalog_manager.default_catalog_name()
    }

    async fn maybe_forward_prom_remote_query(
        &self,
        metric: String,
        req: PrometheusRemoteQueryRequest,
    ) -> Result<Option<ForwardResult<PrometheusRemoteQueryResponse, Error>>> {
        let req_ctx = req.context.as_ref().unwrap();
        let forward_req = ForwardRequest {
            schema: req_ctx.database.clone(),
            table: metric,
            req: req.into_request(),
            forwarded_from: None,
        };
        let do_query = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<PrometheusRemoteQueryRequest>,
                        _: &Endpoint| {
            let query = async move {
                client
                    .prom_remote_query(request)
                    .await
                    .map(|resp| resp.into_inner())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded sql query failed",
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        let forward_result = self.forwarder.forward(forward_req, do_query).await;
        Ok(match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward prom req but the error is ignored, err:{e}");
                None
            }
        })
    }

    /// Returns true when query range maybe exceeding ttl,
    /// Note: False positive is possible
    // TODO(tanruixiang): Add integration testing when supported by the testing
    // framework
    fn check_plan_time_range(
        &self,
        plan: &Plan,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool> {
        if let Plan::Query(query) = &plan {
            // Extract time range represented in timestamp(ms).
            let catalog = self.get_catalog(catalog_name)?;
            let schema = self.get_schema(&catalog, schema_name)?;
            let table_ref = match self.get_table(&schema, table_name) {
                Ok(Some(v)) => v,
                _ => return Ok(false),
            };
            let (start_time, end_time) = extract_start_and_end_time_from_plan(&table_ref, query)?;

            // Stats time infos of queries.
            if let Some(start_time) = &start_time {
                let now = current_time_millis() as f64;
                let start_time = *start_time as f64;
                DURATION_SINCE_QUERY_START_TIME
                    .with_label_values(&[table_ref.name()])
                    .observe(now - start_time);
            }

            if let (Some(start_time), Some(end_time)) = (&start_time, &end_time) {
                let start_time = *start_time as f64;
                let end_time = *end_time as f64;
                QUERY_TIME_RANGE
                    .with_label_values(&[table_ref.name()])
                    .observe(end_time - start_time);
            }

            // Judge whether plan is expired in time.
            if let Some(value) = table_ref.options().get(ENABLE_TTL) {
                if value == "false" {
                    return Ok(false);
                }
            }

            let ttl_duration = if let Some(ttl) = table_ref.options().get(TTL) {
                if let Ok(ttl) = parse_duration(ttl) {
                    ttl
                } else {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            };

            if let Some(end_time) = &end_time {
                let now = current_time_millis() as i64;
                let deadline =
                    now - ttl_duration.as_millis() as i64 - QUERY_EXPIRED_BUFFER.as_millis() as i64;

                if *end_time <= deadline {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn get_catalog(&self, catalog_name: &str) -> Result<CatalogRef> {
        let catalog = self
            .instance
            .catalog_manager
            .catalog_by_name(catalog_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find catalog, catalog_name:{catalog_name}"),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Catalog not found, catalog_name:{catalog_name}"),
            })?;
        Ok(catalog)
    }

    fn get_schema(&self, catalog: &CatalogRef, schema_name: &str) -> Result<SchemaRef> {
        // TODO: support create schema if not exist
        let schema = catalog
            .schema_by_name(schema_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find schema, schema_name:{schema_name}"),
            })?
            .context(ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Schema not found, schema_name:{schema_name}"),
            })?;
        Ok(schema)
    }

    fn get_table(&self, schema: &SchemaRef, table_name: &str) -> Result<Option<TableRef>> {
        let table = schema
            .table_by_name(table_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find table, table_name:{table_name}"),
            })?;
        Ok(table)
    }

    async fn get_partition_table_info(
        &self,
        catalog_name: &str,
        schema_name: &str,
        base_name: &str,
        part_info: &PartitionInfo,
    ) -> Result<TableInfo> {
        let get_inner = |i| async move {
            // TODO: the remote engine should provide a method to get all sub table names.
            let sub_partition_table_name = util::get_sub_partition_name(base_name, part_info, i);
            let table = self
                .instance
                .remote_engine_ref
                .get_table_info(GetTableInfoRequest {
                    table: TableIdentifier {
                        catalog: catalog_name.to_string(),
                        schema: schema_name.to_string(),
                        table: sub_partition_table_name,
                    },
                })
                .await
                .box_err()
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "Failed to get table",
                })?;

            Ok(table)
        };

        let part_num = part_info.get_partition_num();
        // Loop all sub tables to get table info in case of some of them has problems.
        for i in 0..part_num - 1 {
            let ret = get_inner(i).await;
            if let Err(e) = ret {
                warn!("Failed to get table info, err:{e:?}");
            } else {
                return ret;
            }
        }

        // return the last sub table's get result to outside
        get_inner(part_num - 1).await
    }

    async fn maybe_open_partition_table_if_not_exist(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let catalog = self.get_catalog(catalog_name)?;
        let schema = self.get_schema(&catalog, schema_name)?;
        let table = self.get_table(&schema, table_name)?;

        let table_info_in_meta = self
            .router
            .fetch_table_info(schema_name, table_name)
            .await?;

        if let Some(table_info_in_meta) = &table_info_in_meta {
            // No need to handle non-partition table.
            if !table_info_in_meta.is_partition_table() {
                return Ok(());
            }
        }

        match (table, &table_info_in_meta) {
            (Some(table), Some(partition_table_info)) => {
                let table_id = table.id();
                // No need to create partition table when table_id match.
                if table_id == partition_table_info.id {
                    return Ok(());
                }

                info!("Drop partition table because the id of the table in ceresdb is different from the one in ceresmeta,\
                       catalog_name:{catalog_name}, schema_name:{schema_name}, table_name:{table_name}, old_table_id:{table_id}, new_table_id:{}",
                      partition_table_info.id);
                // Drop partition table because the id of the table in ceresdb is different from
                // the one in ceresmeta.
                self.drop_partition_table(
                    schema.clone(),
                    catalog_name.to_string(),
                    schema_name.to_string(),
                    table_name.to_string(),
                )
                .await?;
            }
            (Some(table), None) => {
                // Drop partition table because it does not exist in ceresmeta but exists in
                // ceresdb-server.
                if table.partition_info().is_some() {
                    info!("Drop partition table because it does not exist in ceresmeta but exists in ceresdb-server,\
                           catalog_name:{catalog_name}, schema_name:{schema_name}, table_name:{table_name}, table_id:{}", table.id());
                    self.drop_partition_table(
                        schema.clone(),
                        catalog_name.to_string(),
                        schema_name.to_string(),
                        table_name.to_string(),
                    )
                    .await?;
                }
                // No need to create non-partition table.
                return Ok(());
            }
            // No need to create partition table when table not exist.
            (None, None) => return Ok(()),
            // Create partition table in following code.
            (None, Some(_)) => (),
        }

        let partition_table_info = table_info_in_meta.unwrap();

        // If table not exists, open it.
        let table_info = self
            .get_partition_table_info(
                catalog_name,
                schema_name,
                &partition_table_info.name,
                partition_table_info.partition_info.as_ref().unwrap(),
            )
            .await?;
        // Partition table is a virtual table, so we need to create it manually.
        // Partition info is stored in ceresmeta, so we need to use create_table_request
        // to create it.
        let params = CreateTableParams {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: partition_table_info.name,
            table_schema: table_info.table_schema,
            engine: table_info.engine,
            table_options: table_info.options,
            partition_info: partition_table_info.partition_info,
        };
        let create_table_request = CreateTableRequest {
            params,
            table_id: Some(TableId::new(partition_table_info.id)),
            state: TableState::Stable,
            shard_id: DEFAULT_SHARD_ID,
        };
        let create_opts = CreateOptions {
            table_engine: self.instance.partition_table_engine.clone(),
            create_if_not_exists: true,
        };
        schema
            .create_table(create_table_request.clone(), create_opts)
            .await
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to create table, request:{create_table_request:?}"),
            })?;

        Ok(())
    }

    async fn drop_partition_table(
        &self,
        schema: SchemaRef,
        catalog_name: String,
        schema_name: String,
        table_name: String,
    ) -> Result<()> {
        let opts = DropOptions {
            table_engine: self.instance.partition_table_engine.clone(),
        };
        schema
            .drop_table(
                DropTableRequest {
                    catalog_name,
                    schema_name,
                    table_name: table_name.clone(),
                    engine: PARTITION_TABLE_ENGINE_TYPE.to_string(),
                },
                opts,
            )
            .await
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to drop partition table, table_name:{table_name}"),
            })?;
        Ok(())
    }

    pub(crate) async fn route(&self, req: RouteRequest) -> Result<Vec<Route>> {
        self.router
            .route(req)
            .await
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "fail to route",
            })
    }

    async fn execute_plan(
        &self,
        request_id: RequestId,
        catalog: &str,
        schema: &str,
        plan: Plan,
        deadline: Option<Instant>,
    ) -> Result<Output> {
        self.instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(Internal {
                msg: "Request is blocked",
            })?;

        let interpreter =
            self.build_interpreter(request_id, catalog, schema, plan, deadline, false)?;
        Self::interpreter_execute_plan(interpreter, deadline).await
    }

    async fn execute_plan_involving_partition_table(
        &self,
        request_id: RequestId,
        catalog: &str,
        schema: &str,
        plan: Plan,
        deadline: Option<Instant>,
    ) -> Result<Output> {
        self.instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(Internal {
                msg: "Request is blocked",
            })?;

        let interpreter =
            self.build_interpreter(request_id, catalog, schema, plan, deadline, true)?;
        Self::interpreter_execute_plan(interpreter, deadline).await
    }

    fn build_interpreter(
        &self,
        request_id: RequestId,
        catalog: &str,
        schema: &str,
        plan: Plan,
        deadline: Option<Instant>,
        enable_partition_table_access: bool,
    ) -> Result<InterpreterPtr> {
        let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
            // Use current ctx's catalog and schema as default catalog and schema
            .default_catalog_and_schema(catalog.to_string(), schema.to_string())
            .enable_partition_table_access(enable_partition_table_access)
            .build();
        let interpreter_factory = Factory::new(
            self.instance.query_engine.executor(),
            self.instance.query_engine.physical_planner(),
            self.instance.catalog_manager.clone(),
            self.instance.table_engine.clone(),
            self.instance.table_manipulator.clone(),
        );
        interpreter_factory
            .create(interpreter_ctx, plan)
            .box_err()
            .context(Internal {
                msg: "Failed to create interpreter",
            })
    }

    async fn interpreter_execute_plan(
        interpreter: InterpreterPtr,
        deadline: Option<Instant>,
    ) -> Result<Output> {
        if let Some(deadline) = deadline {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                interpreter.execute(),
            )
            .await
            .box_err()
            .context(Internal {
                msg: "Plan execution timeout",
            })
            .and_then(|v| {
                v.box_err().context(Internal {
                    msg: "Failed to execute interpreter",
                })
            })
        } else {
            interpreter.execute().await.box_err().context(Internal {
                msg: "Failed to execute interpreter",
            })
        }
    }
}

/// Just extract the start and end timestamp(ms) from plan without care about
/// somethings like `Included` and `Excluded`. It can only extract time defined
/// by scalar currently, and `None` will be returned when other exprs found.
fn extract_start_and_end_time_from_plan(
    table_ref: &TableRef,
    query: &QueryPlan,
) -> Result<(Option<i64>, Option<i64>)> {
    let timestamp_name = table_ref
        .schema()
        .column(table_ref.schema().timestamp_index())
        .name
        .clone();
    let ts_col = Column::from_name(timestamp_name);
    let time_range = find_time_range(&query.df_plan, &ts_col)
        .box_err()
        .context(Internal {
            msg: "Failed to find time range",
        })?;

    let start_time = time_expr_to_timestamp(time_range.start, i64::MIN);
    let end_time = time_expr_to_timestamp(time_range.end, i64::MAX);

    Ok((start_time, end_time))
}

fn time_expr_to_timestamp(time_bound_expr: Bound<Expr>, unbounded_val: i64) -> Option<i64> {
    match time_bound_expr {
        Bound::Included(x) | Bound::Excluded(x) => {
            if let Expr::Literal(ScalarValue::Int64(Some(x))) = x {
                Some(x * 1000)
            } else {
                None
            }
        }
        Bound::Unbounded => Some(unbounded_val),
    }
}

#[derive(Clone, Debug)]
pub struct Context {
    request_id: RequestId,
    timeout: Option<Duration>,
    forwarded_from: Option<String>,
}

impl Context {
    pub fn new(timeout: Option<Duration>, forwarded_from: Option<String>) -> Self {
        Self {
            request_id: RequestId::next_id(),
            timeout,
            forwarded_from,
        }
    }
}
