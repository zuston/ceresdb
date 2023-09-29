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

//! Partitioned table supports

pub mod rule;

use bytes_ext::Bytes;
use ceresdbproto::cluster::partition_info::Info;
use macros::define_result;
use regex::Regex;
use snafu::{Backtrace, Snafu};

const PARTITION_TABLE_PREFIX: &str = "__";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to build partition rule, msg:{}.\nBacktrace:{}\n",
        msg,
        backtrace
    ))]
    BuildPartitionRule { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to locate partitions for write, msg:{}.\nBacktrace:{}\n",
        msg,
        backtrace
    ))]
    LocateWritePartition { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to locate partitions for read, msg:{}.\nBacktrace:{}\n",
        msg,
        backtrace
    ))]
    LocateReadPartition { msg: String, backtrace: Backtrace },

    #[snafu(display("Internal error occurred, msg:{}", msg,))]
    Internal { msg: String },

    #[snafu(display("Failed to encode partition info by protobuf, err:{}", source))]
    EncodePartitionInfoToPb { source: prost::EncodeError },

    #[snafu(display(
        "Failed to decode partition info from protobuf bytes, buf:{:?}, err:{}",
        buf,
        source,
    ))]
    DecodePartitionInfoToPb {
        buf: Vec<u8>,
        source: prost::DecodeError,
    },

    #[snafu(display("Encoded partition info content is empty.\nBacktrace:\n{}", backtrace))]
    EmptyEncodedPartitionInfo { backtrace: Backtrace },

    #[snafu(display(
        "Invalid partition info encoding version, version:{}.\nBacktrace:\n{}",
        version,
        backtrace
    ))]
    InvalidPartitionInfoEncodingVersion { version: u8, backtrace: Backtrace },

    #[snafu(display("Partition info could not be empty.\nBacktrace:\n{backtrace}"))]
    EmptyPartitionInfo { backtrace: Backtrace },

    #[snafu(display("Column in the partition key is not found.\nBacktrace:\n{backtrace}"))]
    InvalidPartitionKey { backtrace: Backtrace },
}

define_result!(Error);

/// Info for how to partition table
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionInfo {
    Random(RandomPartitionInfo),
    Hash(HashPartitionInfo),
    Key(KeyPartitionInfo),
}

impl PartitionInfo {
    #[inline]
    pub fn get_definitions(&self) -> Vec<PartitionDefinition> {
        match self {
            Self::Random(v) => v.definitions.clone(),
            Self::Hash(v) => v.definitions.clone(),
            Self::Key(v) => v.definitions.clone(),
        }
    }

    #[inline]
    pub fn get_partition_num(&self) -> usize {
        match self {
            Self::Random(v) => v.definitions.len(),
            Self::Hash(v) => v.definitions.len(),
            Self::Key(v) => v.definitions.len(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PartitionDefinition {
    pub name: String,
    pub origin_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RandomPartitionInfo {
    pub definitions: Vec<PartitionDefinition>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashPartitionInfo {
    pub version: i32,
    pub definitions: Vec<PartitionDefinition>,
    pub expr: Bytes,
    pub linear: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPartitionInfo {
    pub version: i32,
    pub definitions: Vec<PartitionDefinition>,
    pub partition_key: Vec<String>,
    pub linear: bool,
}

impl From<PartitionDefinition> for ceresdbproto::cluster::PartitionDefinition {
    fn from(definition: PartitionDefinition) -> Self {
        Self {
            name: definition.name,
            origin_name: definition
                .origin_name
                .map(ceresdbproto::cluster::partition_definition::OriginName::Origin),
        }
    }
}

impl From<ceresdbproto::cluster::PartitionDefinition> for PartitionDefinition {
    fn from(pb: ceresdbproto::cluster::PartitionDefinition) -> Self {
        let mut origin_name = None;
        if let Some(v) = pb.origin_name {
            match v {
                ceresdbproto::cluster::partition_definition::OriginName::Origin(name) => {
                    origin_name = Some(name)
                }
            }
        }
        Self {
            name: pb.name,
            origin_name,
        }
    }
}

impl From<ceresdbproto::cluster::HashPartitionInfo> for HashPartitionInfo {
    fn from(partition_info_pb: ceresdbproto::cluster::HashPartitionInfo) -> Self {
        HashPartitionInfo {
            version: partition_info_pb.version,
            definitions: partition_info_pb
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            expr: Bytes::from(partition_info_pb.expr),
            linear: partition_info_pb.linear,
        }
    }
}

impl From<HashPartitionInfo> for ceresdbproto::cluster::HashPartitionInfo {
    fn from(partition_info: HashPartitionInfo) -> Self {
        ceresdbproto::cluster::HashPartitionInfo {
            version: partition_info.version,
            definitions: partition_info
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            expr: Bytes::into(partition_info.expr),
            linear: partition_info.linear,
        }
    }
}

impl From<ceresdbproto::cluster::KeyPartitionInfo> for KeyPartitionInfo {
    fn from(partition_info_pb: ceresdbproto::cluster::KeyPartitionInfo) -> Self {
        KeyPartitionInfo {
            version: partition_info_pb.version,
            definitions: partition_info_pb
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            partition_key: partition_info_pb.partition_key,
            linear: partition_info_pb.linear,
        }
    }
}

impl From<KeyPartitionInfo> for ceresdbproto::cluster::KeyPartitionInfo {
    fn from(partition_info: KeyPartitionInfo) -> Self {
        ceresdbproto::cluster::KeyPartitionInfo {
            version: partition_info.version,
            definitions: partition_info
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
            partition_key: partition_info.partition_key,
            linear: partition_info.linear,
        }
    }
}

impl From<ceresdbproto::cluster::RandomPartitionInfo> for RandomPartitionInfo {
    fn from(partition_info_pb: ceresdbproto::cluster::RandomPartitionInfo) -> Self {
        RandomPartitionInfo {
            definitions: partition_info_pb
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
        }
    }
}

impl From<RandomPartitionInfo> for ceresdbproto::cluster::RandomPartitionInfo {
    fn from(partition_info: RandomPartitionInfo) -> Self {
        ceresdbproto::cluster::RandomPartitionInfo {
            definitions: partition_info
                .definitions
                .into_iter()
                .map(|v| v.into())
                .collect(),
        }
    }
}

impl From<PartitionInfo> for ceresdbproto::cluster::PartitionInfo {
    fn from(partition_info: PartitionInfo) -> Self {
        match partition_info {
            PartitionInfo::Hash(v) => {
                let hash_partition_info = ceresdbproto::cluster::HashPartitionInfo::from(v);
                ceresdbproto::cluster::PartitionInfo {
                    info: Some(Info::Hash(hash_partition_info)),
                }
            }
            PartitionInfo::Key(v) => {
                let key_partition_info = ceresdbproto::cluster::KeyPartitionInfo::from(v);
                ceresdbproto::cluster::PartitionInfo {
                    info: Some(Info::Key(key_partition_info)),
                }
            }
            PartitionInfo::Random(v) => {
                let random_partition_info = ceresdbproto::cluster::RandomPartitionInfo::from(v);
                ceresdbproto::cluster::PartitionInfo {
                    info: Some(Info::Random(random_partition_info)),
                }
            }
        }
    }
}

impl TryFrom<ceresdbproto::cluster::PartitionInfo> for PartitionInfo {
    type Error = Error;

    fn try_from(
        partition_info_pb: ceresdbproto::cluster::PartitionInfo,
    ) -> std::result::Result<Self, Self::Error> {
        match partition_info_pb.info {
            Some(info) => match info {
                Info::Hash(v) => {
                    let hash_partition_info = HashPartitionInfo::from(v);
                    Ok(Self::Hash(hash_partition_info))
                }
                Info::Key(v) => {
                    let key_partition_info = KeyPartitionInfo::from(v);
                    Ok(Self::Key(key_partition_info))
                }
                Info::Random(v) => {
                    let random_partition_info = RandomPartitionInfo::from(v);
                    Ok(Self::Random(random_partition_info))
                }
            },
            None => EmptyPartitionInfo {}.fail(),
        }
    }
}

#[inline]
pub fn format_sub_partition_table_name(table_name: &str, partition_name: &str) -> String {
    format!("{PARTITION_TABLE_PREFIX}{table_name}_{partition_name}")
}

#[inline]
pub fn is_sub_partition_table(table_name: &str) -> bool {
    table_name.starts_with(PARTITION_TABLE_PREFIX)
}

pub fn maybe_extract_partitioned_table_name(sub_table_name: &str) -> Option<String> {
    let re = Regex::new(r"__(?P<partitioned_table>\w{1,})_\d{1,}").unwrap();
    let caps = re.captures(sub_table_name)?;
    caps.name("partitioned_table")
        .map(|word| word.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use crate::partition::maybe_extract_partitioned_table_name;

    #[test]
    fn test_extract_partitioned_table_name() {
        let valid_sub_table_name = "__test_0";
        let partitioned_table_name =
            maybe_extract_partitioned_table_name(valid_sub_table_name).unwrap();
        assert_eq!(&partitioned_table_name, "test");

        let valid_sub_table_name = "__test_table_123";
        let partitioned_table_name =
            maybe_extract_partitioned_table_name(valid_sub_table_name).unwrap();
        assert_eq!(&partitioned_table_name, "test_table");

        let normal_table_name = "test";
        let result = maybe_extract_partitioned_table_name(normal_table_name);
        assert!(result.is_none());

        // Just return `None` now.
        let invalid_sub_table_name = "__test1";
        let result = maybe_extract_partitioned_table_name(invalid_sub_table_name);
        assert!(result.is_none());
    }
}
