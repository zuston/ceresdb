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

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::format,
    sync::{
        atomic::{AtomicI32, AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use common_types::schema::Version;
use datafusion::prelude::Expr;
use df_operator::visitor::find_columns_by_expr;
use log::info;
use table_engine::{
    predicate::{Predicate, PredicateRef},
    table::TableStats,
};

use crate::TableStatsOptions;

macro_rules! record_table_stats {
    ($($args:tt)*) => {{
        info!(target: "table_stats", $($args)*);
    }}
}

#[derive(Default)]
pub struct TableStatsImpl {
    pub num_write: AtomicU64,
    pub num_read: AtomicU64,
    pub num_flush: AtomicU64,
    col_filter_stats: Option<ColumnFilterStats>,
}

impl TableStatsImpl {
    pub fn new(table: String, options: TableStatsOptions) -> Self {
        let col_filter_stats = options
            .col_filter_stats_ttl
            .map(|ttl| ColumnFilterStats::new(table, ttl.0));

        Self {
            col_filter_stats,
            ..Default::default()
        }
    }

    pub fn stats_col_filters(&self, predicate: &Predicate) {
        if let Some(stats) = &self.col_filter_stats {
            stats.stats(predicate);
        }
    }
}

impl From<&TableStatsImpl> for TableStats {
    fn from(stats: &TableStatsImpl) -> Self {
        Self {
            num_write: stats.num_write.load(Ordering::Relaxed),
            num_read: stats.num_read.load(Ordering::Relaxed),
            num_flush: stats.num_flush.load(Ordering::Relaxed),
        }
    }
}

// TODO: Maybe we should make it a trait, at least enum.
pub struct ColumnFilterStats {
    table: String,
    ttl: Duration,
    // TODO: so rough now, maybe we can introduce some complete cache framework to replace it(such
    // as `moka`). TODO: maybe unnecessary to stats the timestamp column.
    heats: Mutex<ColumnFilterHeats>,
}

impl ColumnFilterStats {
    pub fn new(table: String, ttl: Duration) -> Self {
        let deadline = Instant::now() + ttl;
        Self {
            table,
            ttl,
            heats: Mutex::new(ColumnFilterHeats::new(deadline)),
        }
    }

    fn stats(&self, predicate: &Predicate) {
        let col_names = Self::get_columns(predicate.exprs());
        self.check_and_update_heats(&col_names);
    }

    fn check_and_update_heats(&self, col_names: &HashSet<String>) {
        dbg!("[Debug] check_and_update_heats");
        let heats = &mut *self.heats.lock().unwrap();

        // If exceeded deadline, pop and print the old heats, build the new heats then.
        let now = Instant::now();
        if now >= heats.deadline {
            dbg!("[Debug] dump");
            let heat_info = heats.dump(&self.table);
            // TODO: just print log now.
            record_table_stats!("TableStats column filter heats:{}", heat_info);

            let new_deadline = now + self.ttl;
            *heats = ColumnFilterHeats::new(new_deadline);
        }

        heats.update_heats(col_names);
    }

    #[inline]
    fn get_columns(exprs: &[Expr]) -> HashSet<String> {
        exprs
            .iter()
            .flat_map(|expr| find_columns_by_expr(expr))
            .collect::<HashSet<_>>()
    }
}

struct ColumnFilterHeats {
    deadline: Instant,
    heats: BTreeMap<String, u32>,
}

impl ColumnFilterHeats {
    fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            heats: BTreeMap::default(),
        }
    }

    fn update_heats(&mut self, col_names: &HashSet<String>) {
        for col in col_names {
            let heat_opt = self.heats.get_mut(col).map(|h| {
                *h += 1;
            });

            if heat_opt.is_none() {
                self.heats.insert(col.clone(), 1);
            }
        }
    }

    fn dump(&self, table: &str) -> String {
        let col_heats = self
            .heats
            .iter()
            .map(|(k, v)| format!("({k}, {v})"))
            .collect::<Vec<_>>()
            .join(",");

        format!("{}:[{col_heats}]", table)
    }
}
