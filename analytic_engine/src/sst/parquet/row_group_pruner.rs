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

// Row group pruner.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use arrow::datatypes::SchemaRef;
use common_types::datum::Datum;
use datafusion::{
    logical_expr::Operator,
    prelude::{lit, Expr},
    scalar::ScalarValue,
};
use log::debug;
use parquet::file::metadata::RowGroupMetaData;
use parquet_ext::prune::{
    equal::{self, ColumnPosition},
    min_max,
};
use snafu::ensure;
use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

use super::meta_data::ColumnValue;
use crate::sst::{
    parquet::meta_data::ParquetFilter,
    reader::error::{OtherNoCause, Result},
};

#[derive(Default, Debug, Clone, TraceMetricWhenDrop)]
struct Metrics {
    #[metric(boolean)]
    use_custom_filter: bool,
    #[metric(number)]
    total_row_groups: usize,
    #[metric(number)]
    row_groups_after_prune: usize,
    #[metric(number)]
    pruned_by_custom_filter: usize,
    #[metric(number)]
    pruned_by_min_max: usize,
    #[metric(collector)]
    collector: Option<MetricsCollector>,
}

/// RowGroupPruner is used to prune row groups according to the provided
/// predicates and filters.
///
/// Currently, two kinds of filters will be applied to such filtering:
/// min max & parquet_filter.
pub struct RowGroupPruner<'a> {
    schema: &'a SchemaRef,
    row_groups: &'a [RowGroupMetaData],
    parquet_filter: Option<&'a ParquetFilter>,
    predicates: Vec<Expr>,
    metrics: Metrics,
}

fn rewrite_expr(expr: Expr, column_values: &HashMap<String, Option<ColumnValue>>) -> Expr {
    let expr = match expr {
        // column not in (v1,v2,v3)
        // ```plaintext
        // [InList(InList { expr: Column(Column { relation: None, name: "name" }),
        //                  list: [Literal(Utf8("v1")), Literal(Utf8("v2")), Literal(Utf8("v3"))],
        //                  negated: true })]
        // ```
        Expr::InList(in_list) => {
            if !in_list.negated {
                return Expr::InList(in_list);
            }

            let column_name = match *in_list.expr.clone() {
                Expr::Column(column) => column.name.to_string(),
                _ => return Expr::InList(in_list),
            };

            let all_possible_values = if let Some(v) = column_values.get(&column_name) {
                v
            } else {
                return Expr::InList(in_list);
            };
            let all_possible_values = if let Some(v) = all_possible_values {
                match v {
                    ColumnValue::StringValue(sv) => sv,
                }
            } else {
                return Expr::InList(in_list);
            };

            let mut not_values = HashSet::new();
            for item in &in_list.list {
                match item {
                    Expr::Literal(scalar_value) => match scalar_value {
                        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
                            if let Some(v) = v {
                                not_values.insert(v.to_string());
                            }
                        }
                        _ => return Expr::InList(in_list),
                    },
                    _ => return Expr::InList(in_list),
                }
            }
            let wanted_values = all_possible_values.difference(&not_values);
            let wanted_values = wanted_values.into_iter().map(lit).collect();
            datafusion::logical_expr::in_list(*in_list.expr, wanted_values, false)
        }
        // column != value
        // ```plaintext
        // [BinaryExpr(BinaryExpr { left: Column(Column { relation: None, name: "name" }),
        //                          op: NotEq,
        //                          right: Literal(Utf8("value")) })]
        // ```
        Expr::BinaryExpr(binary_expr) => {
            if binary_expr.op != Operator::NotEq {
                return Expr::BinaryExpr(binary_expr);
            }

            let column_name = match *binary_expr.left.clone() {
                Expr::Column(column) => column.name.to_string(),
                _ => return Expr::BinaryExpr(binary_expr),
            };
            let all_possible_values = if let Some(v) = column_values.get(&column_name) {
                v
            } else {
                return Expr::BinaryExpr(binary_expr);
            };
            let all_possible_values = if let Some(v) = all_possible_values {
                match v {
                    ColumnValue::StringValue(sv) => sv,
                }
            } else {
                return Expr::BinaryExpr(binary_expr);
            };
            let not_value = match *binary_expr.right.clone() {
                Expr::Literal(scalar_value) => match scalar_value {
                    ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
                        if let Some(v) = v {
                            v.to_string()
                        } else {
                            return Expr::BinaryExpr(binary_expr);
                        }
                    }
                    _ => return Expr::BinaryExpr(binary_expr),
                },
                _ => return Expr::BinaryExpr(binary_expr),
            };
            let wanted_values = all_possible_values
                .iter()
                .filter_map(|value| {
                    if value == &not_value {
                        None
                    } else {
                        Some(lit(value))
                    }
                })
                .collect();
            datafusion::logical_expr::in_list(*binary_expr.left, wanted_values, false)
        }
        _ => expr,
    };

    expr
}
impl<'a> RowGroupPruner<'a> {
    // TODO: DataFusion already change predicates to PhyscialExpr, we should keep up
    // with upstream.
    // https://github.com/apache/arrow-datafusion/issues/4695
    pub fn try_new(
        schema: &'a SchemaRef,
        row_groups: &'a [RowGroupMetaData],
        parquet_filter: Option<&'a ParquetFilter>,
        predicates: &'a [Expr],
        metrics_collector: Option<MetricsCollector>,
        column_values: &'a [Option<ColumnValue>],
    ) -> Result<Self> {
        if let Some(f) = parquet_filter {
            ensure!(f.len() == row_groups.len(), OtherNoCause {
                msg: format!("expect sst_filters.len() == row_groups.len(), num_sst_filters:{}, num_row_groups:{}", f.len(), row_groups.len()),
            });
        }

        ensure!(column_values.len() == schema.fields.len(), OtherNoCause {
            msg: format!("expect column_value.len() == schema_fields.len(), num_sst_filters:{}, num_row_groups:{}", column_values.len(), schema.fields.len()),
        });

        let column_values = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_string(), column_values[i].clone()))
            .collect();
        debug!("Pruner origin predicates:{predicates:?}");
        let predicates = predicates
            .iter()
            .map(|expr| rewrite_expr(expr.clone(), &column_values))
            .collect();
        debug!("Pruner predicates after rewrite:{predicates:?}, column_values:{column_values:?}");

        let metrics = Metrics {
            use_custom_filter: parquet_filter.is_some(),
            total_row_groups: row_groups.len(),
            collector: metrics_collector,
            ..Default::default()
        };

        Ok(Self {
            schema,
            row_groups,
            parquet_filter,
            predicates,
            metrics,
        })
    }

    pub fn prune(&mut self) -> Vec<usize> {
        debug!(
            "Begin to prune row groups, total_row_groups:{}, parquet_filter:{}, predicates:{:?}",
            self.row_groups.len(),
            self.parquet_filter.is_some(),
            self.predicates,
        );

        let pruned0 = self.prune_by_min_max();
        self.metrics.pruned_by_min_max = self.row_groups.len() - pruned0.len();

        let pruned = match self.parquet_filter {
            Some(v) => {
                // TODO: We can do continuous prune based on the `pruned0` to reduce the
                // filtering cost.
                let pruned1 = self.prune_by_filters(v);
                let pruned = Self::intersect_pruned_row_groups(&pruned0, &pruned1);

                self.metrics.pruned_by_custom_filter = self.row_groups.len() - pruned1.len();
                debug!(
                    "Finish pruning row groups by parquet_filter and min_max, total_row_groups:{}, pruned_by_min_max:{}, pruned_by_blooms:{}, pruned_by_both:{}",
                    self.row_groups.len(),
                    pruned0.len(),
                    pruned1.len(),
                    pruned.len(),
                );

                pruned
            }
            None => {
                debug!(
                    "Finish pruning row groups by min_max, total_row_groups:{}, pruned_row_groups:{}",
                    self.row_groups.len(),
                    pruned0.len(),
                );
                pruned0
            }
        };

        self.metrics.row_groups_after_prune = pruned.len();
        pruned
    }

    fn prune_by_min_max(&self) -> Vec<usize> {
        min_max::prune_row_groups(self.schema.clone(), &self.predicates, self.row_groups)
    }

    /// Prune row groups according to the filter.
    fn prune_by_filters(&self, parquet_filter: &ParquetFilter) -> Vec<usize> {
        let is_equal =
            |col_pos: ColumnPosition, val: &ScalarValue, negated: bool| -> Option<bool> {
                let datum = Datum::from_scalar_value(val)?;
                let exist = parquet_filter[col_pos.row_group_idx]
                    .contains_column_data(col_pos.column_idx, &datum.to_bytes())?;
                if exist {
                    // parquet_filter has false positivity, that is to say we are unsure whether
                    // this value exists even if the parquet_filter says it
                    // exists.
                    None
                } else {
                    Some(negated)
                }
            };

        equal::prune_row_groups(
            self.schema.clone(),
            &self.predicates,
            self.row_groups.len(),
            is_equal,
        )
    }

    /// Compute the intersection of the two row groups which are in increasing
    /// order.
    fn intersect_pruned_row_groups(row_groups0: &[usize], row_groups1: &[usize]) -> Vec<usize> {
        let mut intersect = Vec::with_capacity(row_groups0.len().min(row_groups1.len()));

        let (mut i0, mut i1) = (0, 0);
        while i0 < row_groups0.len() && i1 < row_groups1.len() {
            let idx0 = row_groups0[i0];
            let idx1 = row_groups1[i1];

            match idx0.cmp(&idx1) {
                Ordering::Less => i0 += 1,
                Ordering::Greater => i1 += 1,
                Ordering::Equal => {
                    intersect.push(idx0);
                    i0 += 1;
                    i1 += 1;
                }
            }
        }

        intersect
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intersect_row_groups() {
        let test_cases = vec![
            (vec![0, 1, 2, 3, 4], vec![0, 3, 4, 5], vec![0, 3, 4]),
            (vec![], vec![0, 3, 4, 5], vec![]),
            (vec![1, 2, 3], vec![4, 5, 6], vec![]),
            (vec![3], vec![1, 2, 3], vec![3]),
            (vec![4, 5, 6], vec![4, 6, 7], vec![4, 6]),
        ];

        for (row_groups0, row_groups1, expect_row_groups) in test_cases {
            let real_row_groups =
                RowGroupPruner::intersect_pruned_row_groups(&row_groups0, &row_groups1);
            assert_eq!(real_row_groups, expect_row_groups)
        }
    }
}
