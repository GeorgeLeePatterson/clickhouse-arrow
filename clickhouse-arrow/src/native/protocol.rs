use std::str::FromStr;

use strum::AsRefStr;
use uuid::Uuid;

use super::block::Block;
use super::error_codes::map_exception_to_error;
use super::progress::Progress;
use crate::prelude::*;
use crate::{Error, FxIndexMap, Result, ServerError};

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
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY: u64 = 54458;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS: u64 = 54459;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS: u64 = 54460;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES: u64 = 54461;
pub(crate) const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2: u64 = 54462;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS: u64 = 54463;
// pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES: u64 = 54464;
// pub(crate) const DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION: u64 = 54465;
// pub(crate) const DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION: u64 = 54466;
/// Send read-only flag for Replicated tables as well
// pub(crate) const DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK: u64 = 54467;
// pub(crate) const DBMS_MIN_REVISION_WITH_SYSTEM_KEYWORDS_TABLE: u64 = 54468;
pub(crate) const DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION: u64 = 54469;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS: u64 = 54470;
pub(crate) const DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL: u64 = 54471;
/// Push externally granted roles to other nodes
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES: u64 = 54472;
// TODO: Implement other types of json deserialization
// pub(crate) const DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION: u64 = 54473;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_SETTINGS: u64 = 54474;
pub(crate) const DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS: u64 = 54475;
pub(crate) const DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER: u64 = 54476;
pub(crate) const DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION: u64 = 54477;
// pub(crate) const DBMS_MIN_REVISON_WITH_PARALLEL_BLOCK_MARSHALLING: u64 = 54478;
// Current
pub(crate) const DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL: u64 = 54479;

// Active revision
pub(crate) const DBMS_TCP_PROTOCOL_VERSION: u64 =
    DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL;

pub(crate) const DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION: u64 = 4;

// Max size of string over native
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

#[expect(unused)]
#[repr(u64)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum ClientPacketId {
    Hello                     = 0, // Name, version, revision, default DB
    // Query id, query settings, stage up to which the query must be executed, whether the
    // compression must be used, query text (without data for INSERTs).
    Query                     = 1,
    Data                      = 2, // A block of data (compressed or not).
    Cancel                    = 3, // Cancel the query execution.
    Ping                      = 4, // Check that connection to the server is alive.
    TablesStatusRequest       = 5, // Check status of tables on the server.
    KeepAlive                 = 6, // Keep the connection alive
    Scalar                    = 7, // A block of data (compressed or not).
    IgnoredPartUUIDs          = 8, // List of unique parts ids to exclude from query processing
    ReadTaskResponse          = 9, // A filename to read from s3 (used in s3Cluster)
    //Coordinator's decision with a modified set of mark ranges allowed to read
    MergeTreeReadTaskResponse = 10,
    SSHChallengeRequest       = 11, // Request SSH signature challenge
    SSHChallengeResponse      = 12, // Reply to SSH signature challenge
    QueryPlan                 = 13, // Query plan
}

pub(crate) struct ClientHello {
    pub(crate) default_database: String,
    pub(crate) username:         String,
    pub(crate) password:         String,
}

/// `ServerPacketId` is the packet id read from `ClickHouse`.
///
/// See `ServerPacket` for how the data is passed out from the tcp stream's reader
#[repr(u64)]
#[derive(Clone, Copy, Debug, AsRefStr)]
pub(crate) enum ServerPacketId {
    Hello                          = 0,
    Data                           = 1,
    Exception                      = 2,
    Progress                       = 3,
    Pong                           = 4,
    EndOfStream                    = 5,
    ProfileInfo                    = 6,
    Totals                         = 7,
    Extremes                       = 8,
    TablesStatusResponse           = 9,
    Log                            = 10,
    TableColumns                   = 11,
    PartUUIDs                      = 12,
    ReadTaskRequest                = 13,
    ProfileEvents                  = 14,
    MergeTreeAllRangesAnnouncement = 15,
    MergeTreeReadTaskRequest       = 16, // Request from a MergeTree replica to a coordinator
    TimezoneUpdate                 = 17, // Receive server's (session-wide) default timezone
    SSHChallenge                   = 18, // Return challenge for SSH signature signing
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
            15 => ServerPacketId::MergeTreeAllRangesAnnouncement,
            16 => ServerPacketId::MergeTreeReadTaskRequest,
            17 => ServerPacketId::TimezoneUpdate,
            18 => ServerPacketId::SSHChallenge,
            x => {
                error!("invalid packet id from server: {}", x);
                return Err(Error::Protocol(format!("Unknown packet id {i}")));
            }
        })
    }
}

/// The deserialized information read from the tcp stream after a packet id has been received.
#[expect(unused)]
#[derive(Debug, Clone, AsRefStr)]
pub(crate) enum ServerPacket<T = Block> {
    Hello(ServerHello),
    Header(ServerData<Block>),
    Data(ServerData<T>),
    QueryData(ServerData<T>),
    Totals(ServerData<T>),
    Extremes(ServerData<T>),
    ProfileEvents(Vec<ProfileEvent>),
    Log(Vec<LogData>),
    Exception(ServerException),
    Progress(Progress),
    Pong,
    EndOfStream,
    ProfileInfo(ProfileInfo),
    TablesStatusResponse(TablesStatusResponse),
    TableColumns(TableColumns),
    PartUUIDs(Vec<Uuid>),
    ReadTaskRequest(Option<String>),
    MergeTreeAllRangesAnnouncement,
    MergeTreeReadTaskRequest,
    TimezoneUpdate,
    SSHChallenge,
    Ignore(ServerPacketId), // Allows ignoring certain packets
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ServerHello {
    #[expect(unused)]
    pub(crate) server_name:      String,
    #[expect(unused)]
    pub(crate) version:          (u64, u64, u64),
    pub(crate) revision_version: u64,
    #[expect(unused)]
    pub(crate) timezone:         Option<String>,
    #[expect(unused)]
    pub(crate) display_name:     Option<String>,
    pub(crate) settings:         Option<Settings>,
    pub(crate) chunked_send:     ChunkedProtocolMode,
    pub(crate) chunked_recv:     ChunkedProtocolMode,
}

impl ServerHello {
    pub(crate) fn supports_chunked_send(&self) -> bool {
        matches!(
            self.chunked_send,
            ChunkedProtocolMode::Chunked | ChunkedProtocolMode::ChunkedOptional
        )
    }

    pub(crate) fn supports_chunked_recv(&self) -> bool {
        matches!(
            self.chunked_recv,
            ChunkedProtocolMode::Chunked | ChunkedProtocolMode::ChunkedOptional
        )
    }
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
pub(crate) struct ProfileInfo {
    pub(crate) rows:                         u64,
    pub(crate) blocks:                       u64,
    pub(crate) bytes:                        u64,
    pub(crate) applied_limit:                bool,
    pub(crate) rows_before_limit:            u64,
    pub(crate) calculated_rows_before_limit: bool,
    pub(crate) applied_aggregation:          bool,
    pub(crate) rows_before_aggregation:      u64,
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
    fn update_value(&mut self, name: &str, value: Value, type_: &Type) -> Result<()> {
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
    fn update_value(&mut self, name: &str, value: Value, type_: &Type) -> Result<()> {
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

#[derive(Clone, Default, Copy, Debug, PartialEq, Eq, Hash, AsRefStr)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ChunkedProtocolMode {
    #[default]
    #[strum(serialize = "chunked_optional")]
    ChunkedOptional,
    #[strum(serialize = "chunked")]
    Chunked,
    #[strum(serialize = "notchunked_optional")]
    NotChunkedOptional,
    #[strum(serialize = "notchunked")]
    NotChunked,
}

impl ChunkedProtocolMode {
    /// Negotiates chunked protocol between client and server (based on C++ `is_chunked` function)
    pub(crate) fn negotiate(
        server_mode: ChunkedProtocolMode,
        client_mode: ChunkedProtocolMode,
        direction: &str,
    ) -> Result<ChunkedProtocolMode> {
        let server_chunked = matches!(
            server_mode,
            ChunkedProtocolMode::Chunked | ChunkedProtocolMode::ChunkedOptional
        );
        let server_optional = matches!(
            server_mode,
            ChunkedProtocolMode::ChunkedOptional | ChunkedProtocolMode::NotChunkedOptional
        );
        let client_chunked = matches!(
            client_mode,
            ChunkedProtocolMode::Chunked | ChunkedProtocolMode::ChunkedOptional
        );
        let client_optional = matches!(
            client_mode,
            ChunkedProtocolMode::ChunkedOptional | ChunkedProtocolMode::NotChunkedOptional
        );
        let result_chunked = if server_optional {
            client_chunked
        } else if client_optional {
            server_chunked
        } else if client_chunked != server_chunked {
            return Err(Error::Protocol(format!(
                "Incompatible protocol: {} set to {}, server requires {}",
                direction,
                if client_chunked { "chunked" } else { "notchunked" },
                if server_chunked { "chunked" } else { "notchunked" }
            )));
        } else {
            server_chunked
        };

        Ok(if result_chunked {
            ChunkedProtocolMode::Chunked
        } else {
            ChunkedProtocolMode::NotChunked
        })
    }
}

impl FromStr for ChunkedProtocolMode {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "chunked" => Self::Chunked,
            "chunked_optional" => Self::ChunkedOptional,
            "notchunked" => Self::NotChunked,
            "notchunked_optional" => Self::NotChunkedOptional,
            _ => {
                return Err(Error::Protocol(format!(
                    "Unexpected value for chunked protocol mode: {s}"
                )));
            }
        })
    }
}

#[derive(Clone, Default, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CompressionMethod {
    None,
    #[default]
    LZ4,
    ZSTD,
}

impl CompressionMethod {
    pub(crate) fn byte(self) -> u8 {
        match self {
            CompressionMethod::None => 0x02,
            CompressionMethod::LZ4 => 0x82,
            CompressionMethod::ZSTD => 0x90,
        }
    }
}

impl From<&str> for CompressionMethod {
    fn from(value: &str) -> Self {
        match value {
            "lz4" | "LZ4" => CompressionMethod::LZ4,
            "zstd" | "ZSTD" => CompressionMethod::ZSTD,
            _ => CompressionMethod::None,
        }
    }
}

impl std::fmt::Display for CompressionMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionMethod::None => write!(f, "None"),
            CompressionMethod::LZ4 => write!(f, "LZ4"),
            CompressionMethod::ZSTD => write!(f, "ZSTD"),
        }
    }
}

impl FromStr for CompressionMethod {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let method = CompressionMethod::from(s);
        if matches!(method, CompressionMethod::None) {
            return Err(format!("Invalid compression method: {s}"));
        }

        Ok(method)
    }
}

impl AsRef<str> for CompressionMethod {
    fn as_ref(&self) -> &str {
        match self {
            CompressionMethod::None => "None",
            CompressionMethod::LZ4 => "LZ4",
            CompressionMethod::ZSTD => "ZSTD",
        }
    }
}
