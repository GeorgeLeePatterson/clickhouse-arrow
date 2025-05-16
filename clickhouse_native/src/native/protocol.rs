use strum::AsRefStr;
use uuid::Uuid;

use super::block::Block;
use super::error_codes::map_exception_to_error;
use super::progress::Progress;
use crate::prelude::*;
use crate::{ClickhouseNativeError, FxIndexMap, Result, ServerError};

pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_INFO: u64 = 54032;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: u64 = 54058;
pub(crate) const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
// pub(crate) const DBMS_MIN_REVISION_WITH_TABLES_STATUS: u64 = 54226;
// pub(crate) const DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE: u64 = 54337;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: u64 = 54372;
pub(crate) const DBMS_MIN_REVISION_WITH_VERSION_PATCH: u64 = 54401;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_LOGS: u64 = 54406;
// pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_SUPPORT_EMBEDDED_DATA: u64 = 54415;
// pub(crate) const DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD: u64 =
// 54431; pub(crate) const DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA: u64 = 54410;
// pub(crate) const DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE: u64 = 54405;
pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO: u64 = 54420;
pub(crate) const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;
pub(crate) const DBMS_MIN_REVISION_WITH_OPENTELEMETRY: u64 = 54442;
pub(crate) const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET: u64 = 54441;
// pub(crate) const DBMS_MIN_REVISION_WITH_X_FORWARDED_FOR_IN_CLIENT_INFO: u64 = 54443;
// pub(crate) const DBMS_MIN_REVISION_WITH_REFERER_IN_CLIENT_INFO: u64 = 54447;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH: u64 = 54448;

pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME: u64 = 54449;
// pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS: u64 = 54451;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS: u64 = 54453;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT: u64 = 54456;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM: u64 = 54458;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS: u64 = 54459;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS: u64 = 54460;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES: u64 = 54461;
pub(crate) const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2: u64 = 54462;

pub(crate) const DBMS_TCP_PROTOCOL_VERSION: u64 = DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS;
// pub(crate) const DBMS_TCP_PROTOCOL_VERSION: u64 = DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2;

pub(crate) const MAX_STRING_SIZE: usize = 1 << 30;

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
#[expect(unused)]
pub(crate) enum QueryProcessingStage {
    FetchColumns,
    WithMergeableState,
    Complete,
    WithMergableStateAfterAggregation,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum ClientPacketId {
    Hello,
    Query,
    Data,
    Cancel,
    Ping,
    #[expect(unused)]
    TablesStatusRequest,
    #[expect(unused)]
    KeepAlive,
    #[expect(unused)]
    Scalar,
    #[expect(unused)]
    IgnoredPartUUIDs,
    #[expect(unused)]
    ReadTaskResponse,
}

pub(crate) struct ClientHello {
    pub(crate) default_database: String,
    pub(crate) username:         String,
    pub(crate) password:         String,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug, AsRefStr)]
pub(crate) enum ServerPacketId {
    Hello,
    Data,
    Exception,
    Progress,
    Pong,
    EndOfStream,
    ProfileInfo,
    Totals,
    Extremes,
    TablesStatusResponse,
    Log,
    TableColumns,
    PartUUIDs,
    ReadTaskRequest,
    ServerTreeReadTaskRequest,
    ProfileEvents,
    Ignore,
}

impl ServerPacketId {
    pub(crate) fn from_u64(i: u64) -> Result<Self> {
        Ok(match i {
            0 => ServerPacketId::Hello,
            1 => ServerPacketId::Data,
            2 => ServerPacketId::Exception,
            3 => ServerPacketId::Progress,
            4 => ServerPacketId::Pong,
            5 => ServerPacketId::EndOfStream,
            6 => ServerPacketId::ProfileInfo,
            7 => ServerPacketId::Totals,
            8 => ServerPacketId::Extremes,
            9 => ServerPacketId::TablesStatusResponse,
            10 => ServerPacketId::Log,
            11 => ServerPacketId::TableColumns,
            12 => ServerPacketId::PartUUIDs,
            13 => ServerPacketId::ReadTaskRequest,
            14 => ServerPacketId::ProfileEvents,
            15 => ServerPacketId::ServerTreeReadTaskRequest,
            x => {
                error!("invalid packet id from server: {}", x);
                return Err(ClickhouseNativeError::ProtocolError(format!("Unknown packet id {i}")));
            }
        })
    }
}

#[expect(unused)]
#[derive(Debug, Clone, AsRefStr)]
pub(crate) enum ServerPacket<T = Block> {
    Hello(ServerHello),
    Header(ServerData<Block>),
    Data(ServerData<T>),
    Totals(ServerData<T>),
    Extremes(ServerData<T>),
    ProfileEvents(Vec<ProfileEvent>),
    Log(Vec<LogData>),
    Exception(ServerException),
    Progress(Progress),
    Pong,
    EndOfStream,
    ProfileInfo(BlockStreamProfileInfo),
    TablesStatusResponse(TablesStatusResponse),
    TableColumns(TableColumns),
    PartUUIDs(Vec<Uuid>),
    ReadTaskRequest(Option<String>),
    ServerTreeReadTaskRequest,
    Ignore,
}

impl<T> From<&ServerPacket<T>> for ServerPacketId {
    fn from(value: &ServerPacket<T>) -> Self {
        match value {
            ServerPacket::Hello(_) => ServerPacketId::Hello,
            ServerPacket::Header(_) | ServerPacket::Data(_) => ServerPacketId::Data,
            ServerPacket::Exception(_) => ServerPacketId::Exception,
            ServerPacket::Progress(_) => ServerPacketId::Progress,
            ServerPacket::Pong => ServerPacketId::Pong,
            ServerPacket::EndOfStream => ServerPacketId::EndOfStream,
            ServerPacket::ProfileInfo(_) => ServerPacketId::ProfileInfo,
            ServerPacket::Totals(_) => ServerPacketId::Totals,
            ServerPacket::Extremes(_) => ServerPacketId::Extremes,
            ServerPacket::TablesStatusResponse(_) => ServerPacketId::TablesStatusResponse,
            ServerPacket::Log(_) => ServerPacketId::Log,
            ServerPacket::TableColumns(_) => ServerPacketId::TableColumns,
            ServerPacket::PartUUIDs(_) => ServerPacketId::PartUUIDs,
            ServerPacket::ReadTaskRequest(_) => ServerPacketId::ReadTaskRequest,
            ServerPacket::ProfileEvents(_) => ServerPacketId::ProfileEvents,
            ServerPacket::ServerTreeReadTaskRequest => ServerPacketId::ServerTreeReadTaskRequest,
            ServerPacket::Ignore => ServerPacketId::Ignore,
        }
    }
}

#[expect(unused)]
#[derive(Debug, Clone, Default)]
pub(crate) struct ServerHello {
    pub(crate) server_name:      String,
    pub(crate) major_version:    u64,
    pub(crate) minor_version:    u64,
    pub(crate) patch_version:    u64,
    pub(crate) revision_version: u64,
    pub(crate) timezone:         Option<String>,
    pub(crate) display_name:     Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ServerData<T> {
    pub(crate) block: T,
}

#[derive(Debug, Clone)]
pub(crate) struct ServerException {
    pub(crate) code:        i32,
    pub(crate) name:        String,
    pub(crate) message:     String,
    pub(crate) stack_trace: String,
    #[expect(unused)]
    pub(crate) has_nested:  bool,
}

impl ServerException {
    pub(crate) fn emit(self) -> ServerError { map_exception_to_error(self) }
}

#[expect(unused)]
#[derive(Debug, Clone)]
pub(crate) struct BlockStreamProfileInfo {
    pub(crate) rows:                         u64,
    pub(crate) blocks:                       u64,
    pub(crate) bytes:                        u64,
    pub(crate) applied_limit:                bool,
    pub(crate) rows_before_limit:            u64,
    pub(crate) calculated_rows_before_limit: bool,
}

#[expect(unused)]
#[derive(Debug, Clone)]
pub(crate) struct TableColumns {
    pub(crate) name:        String,
    pub(crate) description: String,
}

#[expect(unused)]
#[derive(Debug, Clone)]
pub(crate) struct TableStatus {
    pub(crate) is_replicated:  bool,
    pub(crate) absolute_delay: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct TablesStatusResponse {
    pub(crate) database_tables: FxIndexMap<String, FxIndexMap<String, TableStatus>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct LogData {
    pub(crate) time:       String,
    pub(crate) time_micro: u32,
    pub(crate) host_name:  String,
    pub(crate) query_id:   String,
    pub(crate) thread_id:  u64,
    pub(crate) priority:   i8,
    pub(crate) source:     String,
    pub(crate) text:       String,
}

impl LogData {
    fn update_value(&mut self, name: &str, value: Value, type_: &crate::Type) -> Result<()> {
        match name {
            "time" => self.time = value.to_string(),
            "time_micro" => self.time_micro = value.to_value(type_)?,
            "host_name" => self.host_name = value.to_string(),
            "query_id" => self.query_id = value.to_string(),
            "thread_id" => self.thread_id = value.to_value(type_)?,
            "priority" => self.priority = value.to_value(type_)?,
            "source" => self.source = value.to_string(),
            "text" => self.text = value.to_string(),
            _ => {}
        }
        Ok(())
    }

    #[expect(clippy::cast_possible_truncation)]
    pub(crate) fn from_block(mut block: Block) -> Result<Vec<Self>> {
        let rows = block.rows as usize;
        let mut log_data = vec![Self::default(); rows];
        let mut column_data = std::mem::take(&mut block.column_data);
        for (name, type_) in &block.column_types {
            for (i, value) in column_data.drain(..rows).enumerate() {
                if let Some(log) = log_data.get_mut(i) {
                    log.update_value(name, value, type_)?;
                }
            }
        }
        Ok(log_data)
    }
}

/// Emitted by `ClickHouse` during operations.
#[derive(Debug, Clone, Default)]
pub struct ProfileEvent {
    pub(crate) host_name:    String,
    pub(crate) current_time: String,
    pub(crate) thread_id:    u64,
    pub(crate) type_code:    i8,
    pub(crate) name:         String,
    pub(crate) value:        i64,
}

impl ProfileEvent {
    fn update_value(&mut self, name: &str, value: Value, type_: &crate::Type) -> Result<()> {
        match name {
            "host_name" => self.host_name = value.to_string(),
            "current_time" => self.current_time = value.to_string(),
            "thread_id" => self.thread_id = value.to_value(type_)?,
            "type_code" => self.type_code = value.to_value(type_)?,
            "name" => self.name = value.to_string(),
            "value" => self.value = value.to_value(type_)?,
            _ => {}
        }
        Ok(())
    }

    #[expect(clippy::cast_possible_truncation)]
    pub(crate) fn from_block(mut block: Block) -> Result<Vec<Self>> {
        let rows = block.rows as usize;
        let mut profile_events = vec![Self::default(); rows];
        let mut column_data = std::mem::take(&mut block.column_data);
        for (name, type_) in &block.column_types {
            for (i, value) in column_data.drain(..rows).enumerate() {
                if let Some(profile) = profile_events.get_mut(i) {
                    profile.update_value(name, value, type_).inspect_err(|error| {
                        error!(?error, "profile event update failed");
                    })?;
                }
            }
        }
        Ok(profile_events)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub enum CompressionMethod {
    None,
    #[default]
    LZ4,
}

impl CompressionMethod {
    pub(crate) fn byte(self) -> u8 {
        match self {
            CompressionMethod::None => 0x02,
            CompressionMethod::LZ4 => 0x82,
        }
    }
}

impl From<&str> for CompressionMethod {
    fn from(value: &str) -> Self {
        match value {
            "lz4" | "LZ4" => CompressionMethod::LZ4,
            _ => CompressionMethod::None,
        }
    }
}

impl std::fmt::Display for CompressionMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionMethod::None => write!(f, "None"),
            CompressionMethod::LZ4 => write!(f, "LZ4"),
        }
    }
}

impl std::str::FromStr for CompressionMethod {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lz4" | "LZ4" => Ok(CompressionMethod::LZ4),
            _ => Err(format!("Invalid compression method: {s}")),
        }
    }
}

impl AsRef<str> for CompressionMethod {
    fn as_ref(&self) -> &str {
        match self {
            CompressionMethod::None => "None",
            CompressionMethod::LZ4 => "LZ4",
        }
    }
}
