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

//! Server configs

use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use cluster::config::SchemaConfig;
use common_types::schema::TIMESTAMP_COLUMN;
use meta_client::types::ShardId;
use proxy::{forward, hotspot, SubTableAccessPerm};
use router::{
    endpoint::Endpoint,
    rule_based::{ClusterView, RuleList},
};
use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;
use table_engine::ANALYTIC_ENGINE_TYPE;
use time_ext::ReadableDuration;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct StaticRouteConfig {
    pub rules: RuleList,
    pub topology: StaticTopologyConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardView {
    pub shard_id: ShardId,
    pub endpoint: Endpoint,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
#[serde(default)]
pub struct SchemaShardView {
    pub schema: String,
    pub default_engine_type: String,
    pub default_timestamp_column_name: String,
    pub shard_views: Vec<ShardView>,
}

impl Default for SchemaShardView {
    fn default() -> Self {
        Self {
            schema: "".to_string(),
            default_engine_type: ANALYTIC_ENGINE_TYPE.to_string(),
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
            shard_views: Vec::default(),
        }
    }
}

impl From<SchemaShardView> for SchemaConfig {
    fn from(view: SchemaShardView) -> Self {
        Self {
            default_engine_type: view.default_engine_type,
            default_timestamp_column_name: view.default_timestamp_column_name,
        }
    }
}

#[derive(Debug, Default, Deserialize, Clone, Serialize)]
#[serde(default)]
pub struct StaticTopologyConfig {
    pub schema_shards: Vec<SchemaShardView>,
}

impl From<&StaticTopologyConfig> for ClusterView {
    fn from(config: &StaticTopologyConfig) -> Self {
        let mut schema_configs = HashMap::with_capacity(config.schema_shards.len());
        let mut schema_shards = HashMap::with_capacity(config.schema_shards.len());

        for schema_shard_view in config.schema_shards.clone() {
            let schema = schema_shard_view.schema.clone();
            schema_shards.insert(
                schema.clone(),
                schema_shard_view
                    .shard_views
                    .iter()
                    .map(|shard| (shard.shard_id, shard.endpoint.clone()))
                    .collect(),
            );
            schema_configs.insert(schema, SchemaConfig::from(schema_shard_view));
        }
        ClusterView {
            schema_shards,
            schema_configs,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct QueryDedupConfig {
    pub enable: bool,
    pub notify_timeout: ReadableDuration,
    pub notify_queue_cap: usize,
}

impl Default for QueryDedupConfig {
    fn default() -> Self {
        Self {
            enable: false,
            notify_timeout: ReadableDuration::secs(1),
            notify_queue_cap: 1000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    /// The address to listen.
    pub bind_addr: String,
    pub mysql_port: u16,
    pub postgresql_port: u16,
    pub http_port: u16,
    pub grpc_port: u16,

    pub timeout: Option<ReadableDuration>,
    pub http_max_body_size: ReadableSize,
    pub grpc_server_cq_count: usize,
    /// The minimum length of the response body to compress.
    pub resp_compress_min_length: ReadableSize,

    /// Config for forwarding
    pub forward: forward::Config,

    /// Whether to create table automatically when data is first written, only
    /// used in gRPC
    pub auto_create_table: bool,

    pub default_schema_config: SchemaConfig,

    // Config of route
    pub route_cache: router::RouteCacheConfig,

    /// Record hotspot query or write requests
    pub hotspot: hotspot::Config,

    /// Config of remote engine client
    pub remote_client: remote_engine_client::Config,

    /// Config of dedup query
    pub query_dedup: QueryDedupConfig,

    /// Whether enable to access partition table
    pub sub_table_access_perm: SubTableAccessPerm,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: String::from("127.0.0.1"),
            http_port: 5440,
            mysql_port: 3307,
            postgresql_port: 5433,
            grpc_port: 8831,
            timeout: None,
            http_max_body_size: ReadableSize::mb(64),
            grpc_server_cq_count: 20,
            resp_compress_min_length: ReadableSize::mb(4),
            forward: forward::Config::default(),
            auto_create_table: true,
            default_schema_config: Default::default(),
            route_cache: router::RouteCacheConfig::default(),
            hotspot: hotspot::Config::default(),
            remote_client: remote_engine_client::Config::default(),
            query_dedup: QueryDedupConfig::default(),
            sub_table_access_perm: SubTableAccessPerm::default(),
        }
    }
}

/// Config supporting modifying in runtime
pub struct DynamicConfig {
    pub enable_plan_level_dist_query: Arc<AtomicBool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_endpoint() {
        let cases = [
            (
                "abc.1234.com:1000",
                Endpoint::new("abc.1234.com".to_string(), 1000),
            ),
            (
                "127.0.0.1:1000",
                Endpoint::new("127.0.0.1".to_string(), 1000),
            ),
            (
                "fe80::dce8:23ff:fe0c:f2c0:1000",
                Endpoint::new("fe80::dce8:23ff:fe0c:f2c0".to_string(), 1000),
            ),
        ];

        for (source, expect) in cases {
            let target: Endpoint = source.parse().expect("Should succeed to parse endpoint");
            assert_eq!(target, expect);
        }
    }

    #[test]
    fn test_parse_invalid_endpoint() {
        let cases = [
            "abc.1234.com:1000000",
            "fe80::dce8:23ff:fe0c:f2c0",
            "127.0.0.1",
            "abc.1234.com",
            "abc.1234.com:abcd",
        ];

        for source in cases {
            assert!(source.parse::<Endpoint>().is_err());
        }
    }
}
