use std::borrow::Cow;
use std::num::TryFromIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;

use thiserror::Error;

use crate::Type;
use crate::native::ServerError;

/// Represents various library errors
#[derive(Error, Debug)]
pub enum ClickhouseNativeError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("can't fetch the same column twice from RawRow")]
    DoubleFetch,
    #[error("column index was out of bounds or not present")]
    OutOfBounds,
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("missing connection information")]
    MissingConnectionInformation,
    #[error("malformed connection information: {0}")]
    MalformedConnectionInformation(String),
    #[error("duplicate field {0} in struct")]
    DuplicateField(&'static str),
    #[error("insert block retry")]
    InsertBlockRetry(crate::native::block::Block),
    #[error("protocol error: {0}")]
    ProtocolError(String),
    #[error("Internal channel closed")]
    InternalChannelError,
    #[error("connection timeout: {0}")]
    ConnectionTimeout(String),
    #[error("connection gone: reason = {0}")]
    ConnectionGone(&'static str),
    #[error("type parse error: {0}")]
    TypeParseError(String),
    #[error("deserialize error: {0}")]
    DeserializeError(String),
    #[error("serialize error: {0}")]
    SerializeError(String),
    #[error("deserialize error for column {0}: {1}")]
    DeserializeErrorWithColumn(&'static str, String),
    #[error("connection startup error")]
    StartupError,
    #[error("Exception({0:?})")]
    ServerException(ServerError),
    #[error("unexpected type: {0}")]
    UnexpectedType(Type),
    #[error("unexpected type for column {0}: {1}")]
    UnexpectedTypeWithColumn(Cow<'static, str>, Type),
    #[error("type conversion failure: {0}")]
    TypeConversion(String),
    #[error("str utf-8 conversion error: {0}")]
    Utf8(#[from] Utf8Error),
    #[error("string utf-8 conversion error: {0}")]
    FromUtf8(#[from] FromUtf8Error),
    #[error("Date failed to parse: {0}")]
    DateTime(#[from] TryFromIntError),
    #[error("channel closed")]
    ChannelClosed,
    #[error("Timeout while sending message: {0}")]
    OutgoingTimeout(String),
    #[error("Invalid DNS name: {0}")]
    InvalidDnsName(String),
    #[error("Unsupported setting type: {0}")]
    UnsupportedSettingType(String),
    #[error("Unsupported setting field type: {0}")]
    UnsupportedFieldType(String),
    #[error("No schemas found")]
    UndefinedSchemas,
    #[error("Tables undefined in database {db}: {tables:?}")]
    UndefinedTables { db: String, tables: Vec<String> },
    #[error("Schema configuration is not valid: {0}")]
    SchemaConfig(String),
    #[error("DDL Statement malformed: {0}")]
    DDLMalformed(String),
    #[error("Insufficient scope for ddl queries: {0}")]
    InsufficientDDLScope(String),
    #[error("Client error: {0}")]
    Client(String),

    // Other
    #[error("External error: {0}")]
    External(Box<dyn std::error::Error + Send + Sync>),
    #[error("Unknown error occurred: {0}")]
    Unknown(String),

    // Arrow errors
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("insert block retry")]
    InsertArrowRetry(arrow::record_batch::RecordBatch),
    #[error("arrow serialize error: {0}")]
    ArrowSerialize(String),
    #[error("arrow deserialize error: {0}")]
    ArrowDeserialize(String),
    #[error("Type mismatch: expected {expected}")]
    ArrowTypeMismatch { expected: String, provided: String },
    #[error("Unsupported arrow type: {0}")]
    ArrowUnsupportedType(String),
}

impl ClickhouseNativeError {
    #[must_use]
    pub fn with_column_name(self, name: &'static str) -> Self {
        match self {
            ClickhouseNativeError::DeserializeError(e) => {
                ClickhouseNativeError::DeserializeErrorWithColumn(name, e)
            }
            ClickhouseNativeError::UnexpectedType(e) => {
                ClickhouseNativeError::UnexpectedTypeWithColumn(Cow::Borrowed(name), e)
            }
            x => x,
        }
    }
}

pub type Result<T, E = ClickhouseNativeError> = std::result::Result<T, E>;
