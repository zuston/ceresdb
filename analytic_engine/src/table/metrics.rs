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

//! Metrics of table.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets,
    local::{LocalHistogram, LocalHistogramTimer},
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    Histogram, HistogramTimer, HistogramVec, IntCounter, IntCounterVec,
};
use table_engine::table::TableStats;

use crate::{
    table::{table_stats::TableStatsImpl, version::MemtableStats},
    TableStatsOptions,
};

const KB: f64 = 1024.0;

lazy_static! {
    // Counters:
    static ref TABLE_WRITE_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "table_write_request_counter",
        "Write request counter of table"
    )
    .unwrap();

    static ref TABLE_WRITE_BATCH_HISTOGRAM: Histogram = register_histogram!(
        "table_write_batch_size",
        "Histogram of write batch size",
        vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0]
    )
    .unwrap();

    static ref TABLE_WRITE_FIELDS_COUNTER: IntCounter = register_int_counter!(
        "table_write_fields_counter",
        "Fields counter of table write"
    )
    .unwrap();

    static ref TABLE_READ_REQUEST_COUNTER: IntCounter = register_int_counter!(
        "table_read_request_counter",
        "Read request counter of table"
    )
    .unwrap();

    static ref TABLE_READ_MEMTABLE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "table_read_memtable_counter",
        "Read memtable counter of table",
        &["type"]
    )
    .unwrap();

    // End of counters.

    // Histograms:
    // Buckets: 0, 0.002, .., 0.002 * 4^9
    static ref TABLE_FLUSH_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "table_flush_duration",
        "Histogram for flush duration of the table in seconds",
        exponential_buckets(0.002, 4.0, 10).unwrap()
    ).unwrap();

    // Buckets: 0, 1, .., 2^7
    static ref TABLE_FLUSH_SST_NUM_HISTOGRAM: Histogram = register_histogram!(
        "table_flush_sst_num",
        "Histogram for number of ssts flushed by the table",
        exponential_buckets(1.0, 2.0, 8).unwrap()
    ).unwrap();

    // Buckets: 0, 1, ..., 4^11 (4GB)
    static ref TABLE_FLUSH_SST_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "table_flush_sst_size",
        "Histogram for size of ssts flushed by the table in KB",
        exponential_buckets(1.0, 4.0, 12).unwrap()
    ).unwrap();

    // Buckets: 0, 0.02, .., 0.02 * 4^9
    static ref TABLE_COMPACTION_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "table_compaction_duration",
        "Histogram for compaction duration of the table in seconds",
        exponential_buckets(0.02, 4.0, 10).unwrap()
    ).unwrap();

    // Buckets: 0, 1, .., 2^7
    static ref TABLE_COMPACTION_SST_NUM_HISTOGRAM: Histogram = register_histogram!(
        "table_compaction_sst_num",
        "Histogram for number of ssts compacted by the table",
        exponential_buckets(1.0, 2.0, 8).unwrap()
    ).unwrap();

    // Buckets: 0, 1, ..., 4^11 (4GB)
    static ref TABLE_COMPACTION_SST_SIZE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_compaction_sst_size",
        "Histogram for size of ssts compacted by the table in KB",
        &["type"],
        exponential_buckets(1.0, 4.0, 12).unwrap()
    ).unwrap();

    // Buckets: 0, 1, ..., 10^12(1 billion)
    static ref TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_compaction_sst_row_num",
        "Histogram for row num of ssts compacted by the table",
        &["type"],
        exponential_buckets(1.0, 10.0, 13).unwrap()
    ).unwrap();

    // Buckets: 0, 0.01, .., 0.01 * 2^12
    static ref TABLE_WRITE_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "table_write_duration",
        "Histogram for write stall duration of the table in seconds",
        &["type"],
        exponential_buckets(0.01, 2.0, 13).unwrap()
    ).unwrap();

    // End of histograms.
}

/// Table metrics.
///
/// Now the registered labels won't remove from the metrics vec to avoid panic
/// on concurrent removal.
pub struct Metrics {
    // Stats of a single table.
    pub stats: Arc<TableStatsImpl>,

    compaction_input_sst_size_histogram: Histogram,
    compaction_output_sst_size_histogram: Histogram,
    compaction_input_sst_row_num_histogram: Histogram,
    compaction_output_sst_row_num_histogram: Histogram,

    table_write_stall_duration: Histogram,
    table_write_encode_duration: Histogram,
    table_write_wal_duration: Histogram,
    table_write_memtable_duration: Histogram,
    table_write_preprocess_duration: Histogram,
    table_write_space_flush_wait_duration: Histogram,
    table_write_instance_flush_wait_duration: Histogram,
    table_write_flush_wait_duration: Histogram,
    table_write_execute_duration: Histogram,
    table_write_total_duration: Histogram,

    table_read_mutable_memtable: IntCounter,
    table_read_immutable_memtable: IntCounter,
}

impl Metrics {
    pub fn new(table: String, table_stats_opt: TableStatsOptions) -> Self {
        Self {
            stats: Arc::new(TableStatsImpl::new(table, table_stats_opt)),
            ..Default::default()
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            stats: Arc::new(TableStatsImpl::default()),
            compaction_input_sst_size_histogram: TABLE_COMPACTION_SST_SIZE_HISTOGRAM
                .with_label_values(&["input"]),
            compaction_output_sst_size_histogram: TABLE_COMPACTION_SST_SIZE_HISTOGRAM
                .with_label_values(&["output"]),
            compaction_input_sst_row_num_histogram: TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM
                .with_label_values(&["input"]),
            compaction_output_sst_row_num_histogram: TABLE_COMPACTION_SST_ROW_NUM_HISTOGRAM
                .with_label_values(&["output"]),

            table_write_stall_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["stall"]),
            table_write_encode_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["encode"]),
            table_write_wal_duration: TABLE_WRITE_DURATION_HISTOGRAM.with_label_values(&["wal"]),
            table_write_memtable_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["memtable"]),
            table_write_preprocess_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["preprocess"]),
            table_write_space_flush_wait_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["wait_space_flush"]),
            table_write_instance_flush_wait_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["wait_instance_flush"]),
            table_write_flush_wait_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["wait_flush"]),
            table_write_execute_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["execute"]),
            table_write_total_duration: TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["total"]),

            table_read_mutable_memtable: TABLE_READ_MEMTABLE_COUNTER
                .with_label_values(&["mutable"]),
            table_read_immutable_memtable: TABLE_READ_MEMTABLE_COUNTER
                .with_label_values(&["immutable"]),
        }
    }
}

impl Metrics {
    pub fn table_stats(&self) -> TableStats {
        TableStats::from(&*self.stats)
    }

    #[inline]
    pub fn on_write_request_begin(&self) {
        self.stats.num_write.fetch_add(1, Ordering::Relaxed);
        TABLE_WRITE_REQUEST_COUNTER.inc();
    }

    #[inline]
    pub fn on_write_request_done(&self, num_rows: usize, num_columns: usize) {
        TABLE_WRITE_BATCH_HISTOGRAM.observe(num_rows as f64);
        TABLE_WRITE_FIELDS_COUNTER.inc_by((num_columns * num_rows) as u64);
    }

    #[inline]
    pub fn on_read_request_begin(&self) {
        self.stats.num_read.fetch_add(1, Ordering::Relaxed);
        TABLE_READ_REQUEST_COUNTER.inc();
    }

    #[inline]
    pub fn on_write_stall(&self, duration: Duration) {
        self.table_write_stall_duration
            .observe(duration.as_secs_f64());
    }

    #[inline]
    pub fn start_table_total_timer(&self) -> HistogramTimer {
        self.table_write_total_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_execute_timer(&self) -> HistogramTimer {
        self.table_write_execute_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_encode_timer(&self) -> HistogramTimer {
        self.table_write_encode_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_memtable_timer(&self) -> HistogramTimer {
        self.table_write_memtable_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_wal_timer(&self) -> HistogramTimer {
        self.table_write_wal_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_preprocess_timer(&self) -> HistogramTimer {
        self.table_write_preprocess_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_space_flush_wait_timer(&self) -> HistogramTimer {
        self.table_write_space_flush_wait_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_instance_flush_wait_timer(&self) -> HistogramTimer {
        self.table_write_instance_flush_wait_duration.start_timer()
    }

    #[inline]
    pub fn start_table_write_flush_wait_timer(&self) -> HistogramTimer {
        self.table_write_flush_wait_duration.start_timer()
    }

    #[inline]
    pub fn start_compaction_timer(&self) -> HistogramTimer {
        TABLE_COMPACTION_DURATION_HISTOGRAM.start_timer()
    }

    #[inline]
    pub fn compaction_observe_duration(&self, duration: Duration) {
        TABLE_COMPACTION_DURATION_HISTOGRAM.observe(duration.as_secs_f64());
    }

    #[inline]
    pub fn compaction_observe_sst_num(&self, sst_num: usize) {
        TABLE_COMPACTION_SST_NUM_HISTOGRAM.observe(sst_num as f64);
    }

    #[inline]
    pub fn compaction_observe_input_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.compaction_input_sst_size_histogram
            .observe(sst_size as f64 / KB);
    }

    #[inline]
    pub fn compaction_observe_output_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.compaction_output_sst_size_histogram
            .observe(sst_size as f64 / KB);
    }

    #[inline]
    pub fn compaction_observe_input_sst_row_num(&self, sst_row_num: u64) {
        self.compaction_input_sst_row_num_histogram
            .observe(sst_row_num as f64);
    }

    #[inline]
    pub fn compaction_observe_output_sst_row_num(&self, sst_row_num: u64) {
        self.compaction_output_sst_row_num_histogram
            .observe(sst_row_num as f64);
    }

    #[inline]
    pub fn local_flush_metrics(&self) -> LocalFlushMetrics {
        LocalFlushMetrics {
            stats: self.stats.clone(),
            flush_duration_histogram: TABLE_FLUSH_DURATION_HISTOGRAM.local(),
            flush_sst_num_histogram: TABLE_FLUSH_SST_NUM_HISTOGRAM.local(),
            flush_sst_size_histogram: TABLE_FLUSH_SST_SIZE_HISTOGRAM.local(),
        }
    }

    #[inline]
    pub fn inc_memtable_num(&self, stats: MemtableStats) {
        self.table_read_mutable_memtable
            .inc_by(stats.num_mutable as u64);
        self.table_read_immutable_memtable
            .inc_by(stats.num_immutable as u64);
    }
}

pub struct LocalFlushMetrics {
    stats: Arc<TableStatsImpl>,

    flush_duration_histogram: LocalHistogram,
    flush_sst_num_histogram: LocalHistogram,
    flush_sst_size_histogram: LocalHistogram,
}

impl LocalFlushMetrics {
    pub fn start_flush_timer(&self) -> LocalHistogramTimer {
        self.stats.num_flush.fetch_add(1, Ordering::Relaxed);
        self.flush_duration_histogram.start_timer()
    }

    pub fn observe_sst_num(&self, sst_num: usize) {
        self.flush_sst_num_histogram.observe(sst_num as f64);
    }

    pub fn observe_sst_size(&self, sst_size: u64) {
        // Convert bytes to KB.
        self.flush_sst_size_histogram.observe(sst_size as f64 / KB);
    }
}
