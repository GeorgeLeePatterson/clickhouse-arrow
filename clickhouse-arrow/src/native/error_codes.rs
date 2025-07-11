use std::collections::HashMap;
use std::sync::LazyLock;

use super::protocol::ServerException;
use crate::Error;

/// Represents a server error mapped to internal severity
#[derive(Debug, Clone)]
pub struct ServerError {
    pub error:       Severity,
    pub code:        i32,
    pub name:        String,
    pub message:     String,
    pub stack_trace: String,
}

impl ServerError {
    pub(crate) fn is_fatal(&self) -> bool { matches!(self.error, Severity::Server(_)) }
}

impl From<ServerError> for Error {
    fn from(error: ServerError) -> Self { Error::ServerException(error) }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Exception:")?;
        writeln!(f, "  Error={}", self.error)?;
        writeln!(f, "  Code={}", self.code)?;
        writeln!(f, "  Name={}", self.name)?;
        writeln!(f, "  Message={}", self.message)?;
        writeln!(f, "Stack Trace:")?;
        // Split the stack trace by newlines and format each line with proper indentation
        for line in self.stack_trace.lines() {
            writeln!(f, "  {line}")?;
        }
        Ok(())
    }
}

/// Helper function to match server exception to server error
pub(super) fn map_exception_to_error(exception: ServerException) -> ServerError {
    ServerError {
        error:       map_error_code(exception.code),
        code:        exception.code,
        name:        exception.name,
        message:     exception.message,
        stack_trace: exception.stack_trace,
    }
}

/// Helper to map error codes to `ClickHouseError`
#[expect(clippy::too_many_lines)]
pub(crate) fn map_error_code(code: i32) -> Severity {
    map_error_to_severity(
        match *CLICKHOUSE_ERROR_CODES.get(&code).unwrap_or(&"UNKNOWN_ERROR") {
            // Syntax errors
            "ENGINE_REQUIRED" => ClickHouseError::EngineRequired,
            "SYNTAX_ERROR" => ClickHouseError::SyntaxError,
            "CANNOT_PARSE_TEXT" => ClickHouseError::CannotParseText,
            "CANNOT_PARSE_ESCAPE_SEQUENCE" => ClickHouseError::CannotParseEscapeSequence,
            "CANNOT_PARSE_QUOTED_STRING" => ClickHouseError::CannotParseQuotedString,
            "CANNOT_PARSE_DATE" => ClickHouseError::CannotParseDate,
            "CANNOT_PARSE_DATETIME" => ClickHouseError::CannotParseDateTime,
            "CANNOT_PARSE_NUMBER" => ClickHouseError::CannotParseNumber,
            "CANNOT_PARSE_INPUT_ASSERTION_FAILED" => {
                ClickHouseError::CannotParseInputAssertionFailed
            }
            "CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING" => ClickHouseError::CannotParseDomainValue,
            "CANNOT_PARSE_BOOL" => ClickHouseError::CannotParseBool,
            "ILLEGAL_SYNTAX_FOR_DATA_TYPE" => ClickHouseError::IllegalSyntaxForDataType,
            "ILLEGAL_SYNTAX_FOR_CODEC_TYPE" => ClickHouseError::IllegalSyntaxForCodecType,
            "INVALID_TEMPLATE_FORMAT" => ClickHouseError::InvalidTemplateFormat,
            "MULTIPLE_EXPRESSIONS_FOR_ALIAS" => ClickHouseError::MultipleExpressionsForAlias,

            // Protocol level errors
            "UNKNOWN_COMPRESSION_METHOD" => ClickHouseError::UnknownCompressionMethod,
            "CHECKSUM_DOESNT_MATCH" => ClickHouseError::ChecksumDoesntMatch,
            "UNKNOWN_PACKET_FROM_CLIENT" => ClickHouseError::UnknownPacketFromClient,
            "UNKNOWN_PACKET_FROM_SERVER" => ClickHouseError::UnknownPacketFromServer,
            "UNEXPECTED_PACKET_FROM_CLIENT" => ClickHouseError::UnexpectedPacketFromClient,
            "UNEXPECTED_PACKET_FROM_SERVER" => ClickHouseError::UnexpectedPacketFromServer,
            "TOO_SMALL_BUFFER_SIZE" => ClickHouseError::TooSmallBufferSize,
            "CANNOT_READ_FROM_SOCKET" => ClickHouseError::CannotReadFromSocket,
            "CANNOT_WRITE_TO_SOCKET" => ClickHouseError::CannotWriteToSocket,
            "SOCKET_TIMEOUT" => ClickHouseError::SocketTimeout,
            "NETWORK_ERROR" => ClickHouseError::NetworkError,
            "CLIENT_HAS_CONNECTED_TO_WRONG_PORT" => ClickHouseError::ClientConnectedToWrongPort,
            "PROTOCOL_VERSION_MISMATCH" => ClickHouseError::ProtocolVersionMismatch,

            // Data errors
            "TOO_LARGE_SIZE_COMPRESSED" => ClickHouseError::TooLargeSizeCompressed,
            "DUPLICATE_COLUMN" => ClickHouseError::DuplicateColumn,
            "SIZES_OF_COLUMNS_DOESNT_MATCH" => ClickHouseError::SizesOfColumnsDoesntMatch,
            "NUMBER_OF_COLUMNS_DOESNT_MATCH" => ClickHouseError::NumberOfColumnsDoesntMatch,
            "UNEXPECTED_END_OF_FILE" => ClickHouseError::UnexpectedEOF,
            "SIZE_OF_FIXED_STRING_DOESNT_MATCH" => ClickHouseError::SizeOfFixedStringDoesntMatch,
            "CANNOT_READ_ALL_DATA" => ClickHouseError::CannotReadAllData,
            "INCORRECT_DATA" => ClickHouseError::IncorrectData,
            "INCORRECT_ELEMENT_OF_SET" => ClickHouseError::IncorrectElementOfSet,
            "CORRUPTED_DATA" => ClickHouseError::CorruptedData,
            "SIZES_OF_ARRAYS_DONT_MATCH" => ClickHouseError::SizesOfArraysDontMatch,
            "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" => ClickHouseError::ValueOutOfRangeOfDataType,
            "TOO_LARGE_STRING_SIZE" => ClickHouseError::TooLargeStringSize,
            "DECIMAL_OVERFLOW" => ClickHouseError::DecimalOverflow,
            "EMPTY_DATA_PASSED" => ClickHouseError::EmptyDataPassed,
            "NO_DATA_TO_INSERT" => ClickHouseError::NoDataToInsert,

            // Query level errors
            "UNKNOWN_TYPE" => ClickHouseError::UnknownType,
            "NO_SUCH_COLUMN_IN_TABLE" => ClickHouseError::NoSuchColumnInTable,
            "UNKNOWN_DATABASE" => ClickHouseError::UnknownDatabase,
            "UNKNOWN_TABLE" => ClickHouseError::UnknownTable,
            "THERE_IS_NO_COLUMN" => ClickHouseError::ThereIsNoColumn,
            "NOT_FOUND_COLUMN_IN_BLOCK" => ClickHouseError::NotFoundColumnInBlock,
            "BAD_ARGUMENTS" => ClickHouseError::BadArguments,
            "ILLEGAL_TYPE_OF_ARGUMENT" => ClickHouseError::IllegalTypeOfArgument,
            "TOO_MANY_ARGUMENTS_FOR_FUNCTION" => ClickHouseError::TooManyArgumentsForFunction,
            "TOO_FEW_ARGUMENTS_FOR_FUNCTION" => ClickHouseError::TooFewArgumentsForFunction,
            "UNKNOWN_FUNCTION" => ClickHouseError::UnknownFunction,
            "UNKNOWN_IDENTIFIER" => ClickHouseError::UnknownIdentifier,
            "TYPE_MISMATCH" => ClickHouseError::TypeMismatch,
            "UNKNOWN_SETTING" => ClickHouseError::UnknownSetting,
            "READONLY" => ClickHouseError::Readonly,
            "TABLE_IS_READ_ONLY" => ClickHouseError::TableIsReadOnly,
            "QUERY_IS_TOO_LARGE" => ClickHouseError::QueryIsTooLarge,
            "EMPTY_QUERY" => ClickHouseError::EmptyQuery,
            "TABLE_IS_DROPPED" => ClickHouseError::TableIsDropped,
            "DATABASE_NOT_EMPTY" => ClickHouseError::DatabaseNotEmpty,
            "UNKNOWN_FORMAT" => ClickHouseError::UnknownFormat,
            "TABLE_ALREADY_EXISTS" => ClickHouseError::TableAlreadyExists,
            "DATABASE_ALREADY_EXISTS" => ClickHouseError::DatabaseAlreadyExists,
            "LIMIT_EXCEEDED" => ClickHouseError::LimitExceeded,
            "TIMEOUT_EXCEEDED" => ClickHouseError::TimeoutExceeded,
            "TOO_MANY_ROWS" => ClickHouseError::TooManyRows,
            "TOO_MANY_COLUMNS" => ClickHouseError::TooManyColumns,
            "TOO_DEEP_SUBQUERIES" => ClickHouseError::TooDeepSubqueries,
            "MEMORY_LIMIT_EXCEEDED" => ClickHouseError::MemoryLimitExceeded,
            "UNKNOWN_AGGREGATED_DATA_VARIANT" => ClickHouseError::UnknownAggregatedDataVariant,
            "QUERY_WAS_CANCELLED" => ClickHouseError::QueryWasCancelled,
            "SET_SIZE_LIMIT_EXCEEDED" => ClickHouseError::SetSizeLimitExceeded,
            "INVALID_LIMIT_EXPRESSION" => ClickHouseError::InvalidLimitExpression,

            // Server errors
            "CANNOT_BLOCK_SIGNAL" => ClickHouseError::CannotBlockSignal,
            "CANNOT_UNBLOCK_SIGNAL" => ClickHouseError::CannotUnblockSignal,
            "CANNOT_MANIPULATE_SIGSET" => ClickHouseError::CannotManipulateSigset,
            "CANNOT_WAIT_FOR_SIGNAL" => ClickHouseError::CannotWaitForSignal,
            "THERE_IS_NO_SESSION" => ClickHouseError::ThereIsNoSession,
            "CANNOT_CLOCK_GETTIME" => ClickHouseError::CannotClockGettime,
            "NOT_ENOUGH_SPACE" => ClickHouseError::NotEnoughSpace,
            "CANNOT_ALLOCATE_MEMORY" => ClickHouseError::CannotAllocateMemory,
            "CANNOT_MREMAP" => ClickHouseError::CannotMremap,
            "CANNOT_MUNMAP" => ClickHouseError::CannotMunmap,
            "ABORTED" => ClickHouseError::Aborted,
            "NOT_IMPLEMENTED" => ClickHouseError::NotImplemented,
            "LOGICAL_ERROR" => ClickHouseError::LogicalError,
            "SERVER_OVERLOADED" => ClickHouseError::ServerOverloaded,

            // Authentication errors - treated like protocol errors
            "UNKNOWN_USER" => ClickHouseError::UnknownUser,
            "WRONG_PASSWORD" => ClickHouseError::WrongPassword,
            "REQUIRED_PASSWORD" => ClickHouseError::RequiredPassword,
            "IP_ADDRESS_NOT_ALLOWED" => ClickHouseError::IpAddressNotAllowed,
            "ACCESS_DENIED" => ClickHouseError::AccessDenied,

            "UNKOWN_ERROR" => ClickHouseError::Unknown,
            e => ClickHouseError::Other(e.to_string()),
        },
        code,
    )
}

fn map_error_to_severity(error: ClickHouseError, _code: i32) -> Severity {
    match &error {
        // Syntax errors
        ClickHouseError::SyntaxError
        | ClickHouseError::EngineRequired
        | ClickHouseError::CannotParseText
        | ClickHouseError::CannotParseEscapeSequence
        | ClickHouseError::CannotParseQuotedString
        | ClickHouseError::CannotParseDate
        | ClickHouseError::CannotParseDateTime
        | ClickHouseError::CannotParseNumber
        | ClickHouseError::CannotParseInputAssertionFailed
        | ClickHouseError::CannotParseDomainValue
        | ClickHouseError::CannotParseBool
        | ClickHouseError::IllegalSyntaxForDataType
        | ClickHouseError::IllegalSyntaxForCodecType
        | ClickHouseError::MultipleExpressionsForAlias
        | ClickHouseError::InvalidTemplateFormat => Severity::Syntax(error),

        // Protocol level errors
        ClickHouseError::UnknownCompressionMethod
        | ClickHouseError::ChecksumDoesntMatch
        | ClickHouseError::UnknownPacketFromClient
        | ClickHouseError::UnknownPacketFromServer
        | ClickHouseError::UnexpectedPacketFromClient
        | ClickHouseError::UnexpectedPacketFromServer
        | ClickHouseError::TooSmallBufferSize
        | ClickHouseError::CannotReadFromSocket
        | ClickHouseError::CannotWriteToSocket
        | ClickHouseError::SocketTimeout
        | ClickHouseError::NetworkError
        | ClickHouseError::ClientConnectedToWrongPort
        | ClickHouseError::ProtocolVersionMismatch
        | ClickHouseError::UnknownUser
        | ClickHouseError::WrongPassword
        | ClickHouseError::RequiredPassword
        | ClickHouseError::IpAddressNotAllowed
        | ClickHouseError::AccessDenied => Severity::Protocol(error),

        // Data errors
        ClickHouseError::TooLargeSizeCompressed
        | ClickHouseError::DuplicateColumn
        | ClickHouseError::SizesOfColumnsDoesntMatch
        | ClickHouseError::NumberOfColumnsDoesntMatch
        | ClickHouseError::UnexpectedEOF
        | ClickHouseError::SizeOfFixedStringDoesntMatch
        | ClickHouseError::CannotReadAllData
        | ClickHouseError::IncorrectData
        | ClickHouseError::IncorrectElementOfSet
        | ClickHouseError::CorruptedData
        | ClickHouseError::SizesOfArraysDontMatch
        | ClickHouseError::ValueOutOfRangeOfDataType
        | ClickHouseError::TooLargeStringSize
        | ClickHouseError::DecimalOverflow
        | ClickHouseError::EmptyDataPassed
        | ClickHouseError::NoDataToInsert => Severity::Data(error),

        // Query level errors
        ClickHouseError::UnknownType
        | ClickHouseError::NoSuchColumnInTable
        | ClickHouseError::ThereIsNoColumn
        | ClickHouseError::NotFoundColumnInBlock
        | ClickHouseError::BadArguments
        | ClickHouseError::IllegalTypeOfArgument
        | ClickHouseError::TooManyArgumentsForFunction
        | ClickHouseError::TooFewArgumentsForFunction
        | ClickHouseError::UnknownFunction
        | ClickHouseError::UnknownIdentifier
        | ClickHouseError::TypeMismatch
        | ClickHouseError::UnknownSetting
        | ClickHouseError::Readonly
        | ClickHouseError::TableIsReadOnly
        | ClickHouseError::QueryIsTooLarge
        | ClickHouseError::EmptyQuery
        | ClickHouseError::TableIsDropped
        | ClickHouseError::DatabaseNotEmpty
        | ClickHouseError::UnknownFormat
        | ClickHouseError::UnknownDatabase
        | ClickHouseError::UnknownTable
        | ClickHouseError::TableAlreadyExists
        | ClickHouseError::DatabaseAlreadyExists
        | ClickHouseError::LimitExceeded
        | ClickHouseError::TimeoutExceeded
        | ClickHouseError::TooManyRows
        | ClickHouseError::TooManyColumns
        | ClickHouseError::TooDeepSubqueries
        | ClickHouseError::MemoryLimitExceeded
        | ClickHouseError::UnknownAggregatedDataVariant
        | ClickHouseError::QueryWasCancelled
        | ClickHouseError::SetSizeLimitExceeded
        | ClickHouseError::Aborted
        | ClickHouseError::NotImplemented
        | ClickHouseError::LogicalError
        | ClickHouseError::InvalidLimitExpression => Severity::Query(error),

        // Server errors - be conservative about what goes here
        ClickHouseError::CannotBlockSignal
        | ClickHouseError::CannotUnblockSignal
        | ClickHouseError::CannotManipulateSigset
        | ClickHouseError::CannotWaitForSignal
        | ClickHouseError::ThereIsNoSession
        | ClickHouseError::CannotClockGettime
        | ClickHouseError::NotEnoughSpace
        | ClickHouseError::CannotAllocateMemory
        | ClickHouseError::CannotMremap
        | ClickHouseError::CannotMunmap
        | ClickHouseError::ServerOverloaded => Severity::Server(error),

        // Unknown
        _ => Severity::Unknown(error),
    }
}

/// Wrapper to indicate level of severity of error
#[derive(thiserror::Error, Debug, Clone)]
pub enum Severity {
    #[error("Syntax({0:?})")]
    Syntax(ClickHouseError),
    #[error("Query({0:?})")]
    Query(ClickHouseError),
    #[error("Data({0:?})")]
    Data(ClickHouseError),
    #[error("Protocol({0:?})")]
    Protocol(ClickHouseError),
    #[error("Server({0:?})")]
    Server(ClickHouseError),
    #[error("Unknown({0:?})")]
    Unknown(ClickHouseError),
}

/// Common error codes mapped to concrete errors. Can be updated as needed.
#[non_exhaustive]
#[derive(thiserror::Error, Debug, Clone)]
pub enum ClickHouseError {
    #[error("Engine required")]
    EngineRequired,
    #[error("Compressed size too large")]
    TooLargeSizeCompressed,
    #[error("Unknown type")]
    UnknownType,
    #[error("Duplicate column")]
    DuplicateColumn,
    #[error("Column not found in table")]
    NoSuchColumnInTable,
    #[error("Column row lengths do not match")]
    SizesOfColumnsDoesntMatch,
    #[error("Column counts do not match")]
    NumberOfColumnsDoesntMatch,
    #[error("Unexpected end of file")]
    UnexpectedEOF,
    #[error("Database is unknown")]
    UnknownDatabase,
    #[error("Table is unknown")]
    UnknownTable,
    #[error("Unknown compression method")]
    UnknownCompressionMethod,
    #[error("Syntax error")]
    SyntaxError,

    // Signal handling errors
    #[error("Cannot block signal")]
    CannotBlockSignal,
    #[error("Cannot unblock signal")]
    CannotUnblockSignal,
    #[error("Cannot manipulate signal set")]
    CannotManipulateSigset,
    #[error("Cannot wait for signal")]
    CannotWaitForSignal,
    #[error("No session available")]
    ThereIsNoSession,
    #[error("Cannot get clock time")]
    CannotClockGettime,
    #[error("Unknown setting")]
    UnknownSetting,

    // Parse errors
    #[error("Cannot parse text")]
    CannotParseText,
    #[error("Cannot parse escape sequence")]
    CannotParseEscapeSequence,
    #[error("Cannot parse quoted string")]
    CannotParseQuotedString,
    #[error("Cannot parse input assertion failed")]
    CannotParseInputAssertionFailed,
    #[error("Cannot parse date")]
    CannotParseDate,
    #[error("Cannot parse datetime")]
    CannotParseDateTime,
    #[error("Cannot parse number")]
    CannotParseNumber,
    #[error("Cannot parse domain value from string")]
    CannotParseDomainValue,
    #[error("Cannot parse boolean")]
    CannotParseBool,

    // Column errors
    #[error("There is no column")]
    ThereIsNoColumn,
    #[error("Column not found in block")]
    NotFoundColumnInBlock,
    #[error("Size of fixed string doesn't match")]
    SizeOfFixedStringDoesntMatch,

    // Data errors
    #[error("Cannot read all data")]
    CannotReadAllData,
    #[error("Incorrect data")]
    IncorrectData,
    #[error("Incorrect element of set")]
    IncorrectElementOfSet,
    #[error("Corrupted data")]
    CorruptedData,
    #[error("Sizes of arrays don't match")]
    SizesOfArraysDontMatch,
    #[error("Value is out of range of data type")]
    ValueOutOfRangeOfDataType,
    #[error("String size too large")]
    TooLargeStringSize,
    #[error("Decimal overflow")]
    DecimalOverflow,
    #[error("Empty data passed")]
    EmptyDataPassed,
    #[error("No data to insert")]
    NoDataToInsert,

    // Function errors
    #[error("Bad arguments")]
    BadArguments,
    #[error("Illegal type of argument")]
    IllegalTypeOfArgument,
    #[error("Too many arguments for function")]
    TooManyArgumentsForFunction,
    #[error("Too few arguments for function")]
    TooFewArgumentsForFunction,
    #[error("Unknown function")]
    UnknownFunction,
    #[error("Unknown identifier")]
    UnknownIdentifier,
    #[error("Type mismatch")]
    TypeMismatch,

    // Query errors
    #[error("Read-only mode")]
    Readonly,
    #[error("Table is read only")]
    TableIsReadOnly,
    #[error("Query is too large")]
    QueryIsTooLarge,
    #[error("Empty query")]
    EmptyQuery,
    #[error("Table is dropped")]
    TableIsDropped,
    #[error("Database not empty")]
    DatabaseNotEmpty,
    #[error("Unknown format")]
    UnknownFormat,
    #[error("Table already exists")]
    TableAlreadyExists,
    #[error("Database already exists")]
    DatabaseAlreadyExists,
    #[error("Limit exceeded")]
    LimitExceeded,
    #[error("Timeout exceeded")]
    TimeoutExceeded,
    #[error("Too many rows")]
    TooManyRows,
    #[error("Too many columns")]
    TooManyColumns,
    #[error("Subqueries too deeply nested")]
    TooDeepSubqueries,
    #[error("Memory limit exceeded")]
    MemoryLimitExceeded,
    #[error("Unknown aggregated data variant")]
    UnknownAggregatedDataVariant,
    #[error("Query was cancelled")]
    QueryWasCancelled,
    #[error("Set size limit exceeded")]
    SetSizeLimitExceeded,
    #[error("Invalid limit expression")]
    InvalidLimitExpression,

    // Protocol errors
    #[error("Checksum doesn't match")]
    ChecksumDoesntMatch,
    #[error("Unknown packet from client")]
    UnknownPacketFromClient,
    #[error("Unknown packet from server")]
    UnknownPacketFromServer,
    #[error("Unexpected packet from client")]
    UnexpectedPacketFromClient,
    #[error("Unexpected packet from server")]
    UnexpectedPacketFromServer,
    #[error("Buffer size too small")]
    TooSmallBufferSize,
    #[error("Cannot read from socket")]
    CannotReadFromSocket,
    #[error("Cannot write to socket")]
    CannotWriteToSocket,
    #[error("Socket timeout")]
    SocketTimeout,
    #[error("Network error")]
    NetworkError,
    #[error("Client has connected to the wrong port")]
    ClientConnectedToWrongPort,
    #[error("Protocol version mismatch")]
    ProtocolVersionMismatch,

    // Authentication errors
    #[error("Unknown user")]
    UnknownUser,
    #[error("Wrong password")]
    WrongPassword,
    #[error("Password required")]
    RequiredPassword,
    #[error("IP address not allowed")]
    IpAddressNotAllowed,
    #[error("Access denied")]
    AccessDenied,

    // Server condition errors
    #[error("Not enough space")]
    NotEnoughSpace,
    #[error("Cannot allocate memory")]
    CannotAllocateMemory,
    #[error("Cannot mremap")]
    CannotMremap,
    #[error("Cannot munmap")]
    CannotMunmap,
    #[error("Operation aborted")]
    Aborted,
    #[error("Not implemented")]
    NotImplemented,
    #[error("Logical error")]
    LogicalError,
    #[error("Server overloaded")]
    ServerOverloaded,

    // Syntax errors
    #[error("Illegal syntax for data type")]
    IllegalSyntaxForDataType,
    #[error("Illegal syntax for codec type")]
    IllegalSyntaxForCodecType,
    #[error("Invalid template format")]
    InvalidTemplateFormat,
    #[error("Multiple expressions for alias")]
    MultipleExpressionsForAlias,

    #[error("Other error: {0}")]
    Other(String),
    #[error("Unknown error")]
    Unknown,
}

pub(crate) static CLICKHOUSE_ERROR_CODES: LazyLock<HashMap<i32, &'static str>> =
    LazyLock::new(|| {
        HashMap::from_iter(vec![
            (0, "OK"),
            (1, "UNSUPPORTED_METHOD"),
            (2, "UNSUPPORTED_PARAMETER"),
            (3, "UNEXPECTED_END_OF_FILE"),
            (4, "EXPECTED_END_OF_FILE"),
            (6, "CANNOT_PARSE_TEXT"),
            (7, "INCORRECT_NUMBER_OF_COLUMNS"),
            (8, "THERE_IS_NO_COLUMN"),
            (9, "SIZES_OF_COLUMNS_DOESNT_MATCH"),
            (10, "NOT_FOUND_COLUMN_IN_BLOCK"),
            (11, "POSITION_OUT_OF_BOUND"),
            (12, "PARAMETER_OUT_OF_BOUND"),
            (13, "SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH"),
            (15, "DUPLICATE_COLUMN"),
            (16, "NO_SUCH_COLUMN_IN_TABLE"),
            (19, "SIZE_OF_FIXED_STRING_DOESNT_MATCH"),
            (20, "NUMBER_OF_COLUMNS_DOESNT_MATCH"),
            (23, "CANNOT_READ_FROM_ISTREAM"),
            (24, "CANNOT_WRITE_TO_OSTREAM"),
            (25, "CANNOT_PARSE_ESCAPE_SEQUENCE"),
            (26, "CANNOT_PARSE_QUOTED_STRING"),
            (27, "CANNOT_PARSE_INPUT_ASSERTION_FAILED"),
            (28, "CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER"),
            (32, "ATTEMPT_TO_READ_AFTER_EOF"),
            (33, "CANNOT_READ_ALL_DATA"),
            (34, "TOO_MANY_ARGUMENTS_FOR_FUNCTION"),
            (35, "TOO_FEW_ARGUMENTS_FOR_FUNCTION"),
            (36, "BAD_ARGUMENTS"),
            (37, "UNKNOWN_ELEMENT_IN_AST"),
            (38, "CANNOT_PARSE_DATE"),
            (39, "TOO_LARGE_SIZE_COMPRESSED"),
            (40, "CHECKSUM_DOESNT_MATCH"),
            (41, "CANNOT_PARSE_DATETIME"),
            (42, "NUMBER_OF_ARGUMENTS_DOESNT_MATCH"),
            (43, "ILLEGAL_TYPE_OF_ARGUMENT"),
            (44, "ILLEGAL_COLUMN"),
            (46, "UNKNOWN_FUNCTION"),
            (47, "UNKNOWN_IDENTIFIER"),
            (48, "NOT_IMPLEMENTED"),
            (49, "LOGICAL_ERROR"),
            (50, "UNKNOWN_TYPE"),
            (51, "EMPTY_LIST_OF_COLUMNS_QUERIED"),
            (52, "COLUMN_QUERIED_MORE_THAN_ONCE"),
            (53, "TYPE_MISMATCH"),
            (55, "STORAGE_REQUIRES_PARAMETER"),
            (56, "UNKNOWN_STORAGE"),
            (57, "TABLE_ALREADY_EXISTS"),
            (58, "TABLE_METADATA_ALREADY_EXISTS"),
            (59, "ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER"),
            (60, "UNKNOWN_TABLE"),
            (62, "SYNTAX_ERROR"),
            (63, "UNKNOWN_AGGREGATE_FUNCTION"),
            (68, "CANNOT_GET_SIZE_OF_FIELD"),
            (69, "ARGUMENT_OUT_OF_BOUND"),
            (70, "CANNOT_CONVERT_TYPE"),
            (71, "CANNOT_WRITE_AFTER_END_OF_BUFFER"),
            (72, "CANNOT_PARSE_NUMBER"),
            (73, "UNKNOWN_FORMAT"),
            (74, "CANNOT_READ_FROM_FILE_DESCRIPTOR"),
            (75, "CANNOT_WRITE_TO_FILE_DESCRIPTOR"),
            (76, "CANNOT_OPEN_FILE"),
            (77, "CANNOT_CLOSE_FILE"),
            (78, "UNKNOWN_TYPE_OF_QUERY"),
            (79, "INCORRECT_FILE_NAME"),
            (80, "INCORRECT_QUERY"),
            (81, "UNKNOWN_DATABASE"),
            (82, "DATABASE_ALREADY_EXISTS"),
            (83, "DIRECTORY_DOESNT_EXIST"),
            (84, "DIRECTORY_ALREADY_EXISTS"),
            (85, "FORMAT_IS_NOT_SUITABLE_FOR_INPUT"),
            (86, "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER"),
            (87, "CANNOT_SEEK_THROUGH_FILE"),
            (88, "CANNOT_TRUNCATE_FILE"),
            (89, "UNKNOWN_COMPRESSION_METHOD"),
            (90, "EMPTY_LIST_OF_COLUMNS_PASSED"),
            (91, "SIZES_OF_MARKS_FILES_ARE_INCONSISTENT"),
            (92, "EMPTY_DATA_PASSED"),
            (93, "UNKNOWN_AGGREGATED_DATA_VARIANT"),
            (94, "CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS"),
            (95, "CANNOT_READ_FROM_SOCKET"),
            (96, "CANNOT_WRITE_TO_SOCKET"),
            (99, "UNKNOWN_PACKET_FROM_CLIENT"),
            (100, "UNKNOWN_PACKET_FROM_SERVER"),
            (101, "UNEXPECTED_PACKET_FROM_CLIENT"),
            (102, "UNEXPECTED_PACKET_FROM_SERVER"),
            (104, "TOO_SMALL_BUFFER_SIZE"),
            (107, "FILE_DOESNT_EXIST"),
            (108, "NO_DATA_TO_INSERT"),
            (109, "CANNOT_BLOCK_SIGNAL"),
            (110, "CANNOT_UNBLOCK_SIGNAL"),
            (111, "CANNOT_MANIPULATE_SIGSET"),
            (112, "CANNOT_WAIT_FOR_SIGNAL"),
            (113, "THERE_IS_NO_SESSION"),
            (114, "CANNOT_CLOCK_GETTIME"),
            (115, "UNKNOWN_SETTING"),
            (116, "THERE_IS_NO_DEFAULT_VALUE"),
            (117, "INCORRECT_DATA"),
            (119, "ENGINE_REQUIRED"),
            (120, "CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE"),
            (121, "UNSUPPORTED_JOIN_KEYS"),
            (122, "INCOMPATIBLE_COLUMNS"),
            (123, "UNKNOWN_TYPE_OF_AST_NODE"),
            (124, "INCORRECT_ELEMENT_OF_SET"),
            (125, "INCORRECT_RESULT_OF_SCALAR_SUBQUERY"),
            (127, "ILLEGAL_INDEX"),
            (128, "TOO_LARGE_ARRAY_SIZE"),
            (129, "FUNCTION_IS_SPECIAL"),
            (130, "CANNOT_READ_ARRAY_FROM_TEXT"),
            (131, "TOO_LARGE_STRING_SIZE"),
            (133, "AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS"),
            (134, "PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS"),
            (135, "ZERO_ARRAY_OR_TUPLE_INDEX"),
            (137, "UNKNOWN_ELEMENT_IN_CONFIG"),
            (138, "EXCESSIVE_ELEMENT_IN_CONFIG"),
            (139, "NO_ELEMENTS_IN_CONFIG"),
            (141, "SAMPLING_NOT_SUPPORTED"),
            (142, "NOT_FOUND_NODE"),
            (145, "UNKNOWN_OVERFLOW_MODE"),
            (152, "UNKNOWN_DIRECTION_OF_SORTING"),
            (153, "ILLEGAL_DIVISION"),
            (156, "DICTIONARIES_WAS_NOT_LOADED"),
            (158, "TOO_MANY_ROWS"),
            (159, "TIMEOUT_EXCEEDED"),
            (160, "TOO_SLOW"),
            (161, "TOO_MANY_COLUMNS"),
            (162, "TOO_DEEP_SUBQUERIES"),
            (164, "READONLY"),
            (165, "TOO_MANY_TEMPORARY_COLUMNS"),
            (166, "TOO_MANY_TEMPORARY_NON_CONST_COLUMNS"),
            (167, "TOO_DEEP_AST"),
            (168, "TOO_BIG_AST"),
            (169, "BAD_TYPE_OF_FIELD"),
            (170, "BAD_GET"),
            (172, "CANNOT_CREATE_DIRECTORY"),
            (173, "CANNOT_ALLOCATE_MEMORY"),
            (174, "CYCLIC_ALIASES"),
            (179, "MULTIPLE_EXPRESSIONS_FOR_ALIAS"),
            (180, "THERE_IS_NO_PROFILE"),
            (181, "ILLEGAL_FINAL"),
            (182, "ILLEGAL_PREWHERE"),
            (183, "UNEXPECTED_EXPRESSION"),
            (184, "ILLEGAL_AGGREGATION"),
            (186, "UNSUPPORTED_COLLATION_LOCALE"),
            (187, "COLLATION_COMPARISON_FAILED"),
            (190, "SIZES_OF_ARRAYS_DONT_MATCH"),
            (191, "SET_SIZE_LIMIT_EXCEEDED"),
            (192, "UNKNOWN_USER"),
            (193, "WRONG_PASSWORD"),
            (194, "REQUIRED_PASSWORD"),
            (195, "IP_ADDRESS_NOT_ALLOWED"),
            (196, "UNKNOWN_ADDRESS_PATTERN_TYPE"),
            (198, "DNS_ERROR"),
            (199, "UNKNOWN_QUOTA"),
            (201, "QUOTA_EXCEEDED"),
            (202, "TOO_MANY_SIMULTANEOUS_QUERIES"),
            (203, "NO_FREE_CONNECTION"),
            (204, "CANNOT_FSYNC"),
            (206, "ALIAS_REQUIRED"),
            (207, "AMBIGUOUS_IDENTIFIER"),
            (208, "EMPTY_NESTED_TABLE"),
            (209, "SOCKET_TIMEOUT"),
            (210, "NETWORK_ERROR"),
            (211, "EMPTY_QUERY"),
            (212, "UNKNOWN_LOAD_BALANCING"),
            (213, "UNKNOWN_TOTALS_MODE"),
            (214, "CANNOT_STATVFS"),
            (215, "NOT_AN_AGGREGATE"),
            (216, "QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING"),
            (217, "CLIENT_HAS_CONNECTED_TO_WRONG_PORT"),
            (218, "TABLE_IS_DROPPED"),
            (219, "DATABASE_NOT_EMPTY"),
            (220, "DUPLICATE_INTERSERVER_IO_ENDPOINT"),
            (221, "NO_SUCH_INTERSERVER_IO_ENDPOINT"),
            (223, "UNEXPECTED_AST_STRUCTURE"),
            (224, "REPLICA_IS_ALREADY_ACTIVE"),
            (225, "NO_ZOOKEEPER"),
            (226, "NO_FILE_IN_DATA_PART"),
            (227, "UNEXPECTED_FILE_IN_DATA_PART"),
            (228, "BAD_SIZE_OF_FILE_IN_DATA_PART"),
            (229, "QUERY_IS_TOO_LARGE"),
            (230, "NOT_FOUND_EXPECTED_DATA_PART"),
            (231, "TOO_MANY_UNEXPECTED_DATA_PARTS"),
            (232, "NO_SUCH_DATA_PART"),
            (233, "BAD_DATA_PART_NAME"),
            (234, "NO_REPLICA_HAS_PART"),
            (235, "DUPLICATE_DATA_PART"),
            (236, "ABORTED"),
            (237, "NO_REPLICA_NAME_GIVEN"),
            (238, "FORMAT_VERSION_TOO_OLD"),
            (239, "CANNOT_MUNMAP"),
            (240, "CANNOT_MREMAP"),
            (241, "MEMORY_LIMIT_EXCEEDED"),
            (242, "TABLE_IS_READ_ONLY"),
            (243, "NOT_ENOUGH_SPACE"),
            (244, "UNEXPECTED_ZOOKEEPER_ERROR"),
            (246, "CORRUPTED_DATA"),
            (248, "INVALID_PARTITION_VALUE"),
            (251, "NO_SUCH_REPLICA"),
            (252, "TOO_MANY_PARTS"),
            (253, "REPLICA_ALREADY_EXISTS"),
            (254, "NO_ACTIVE_REPLICAS"),
            (255, "TOO_MANY_RETRIES_TO_FETCH_PARTS"),
            (256, "PARTITION_ALREADY_EXISTS"),
            (257, "PARTITION_DOESNT_EXIST"),
            (258, "UNION_ALL_RESULT_STRUCTURES_MISMATCH"),
            (260, "CLIENT_OUTPUT_FORMAT_SPECIFIED"),
            (261, "UNKNOWN_BLOCK_INFO_FIELD"),
            (262, "BAD_COLLATION"),
            (263, "CANNOT_COMPILE_CODE"),
            (264, "INCOMPATIBLE_TYPE_OF_JOIN"),
            (265, "NO_AVAILABLE_REPLICA"),
            (266, "MISMATCH_REPLICAS_DATA_SOURCES"),
            (269, "INFINITE_LOOP"),
            (270, "CANNOT_COMPRESS"),
            (271, "CANNOT_DECOMPRESS"),
            (272, "CANNOT_IO_SUBMIT"),
            (273, "CANNOT_IO_GETEVENTS"),
            (274, "AIO_READ_ERROR"),
            (275, "AIO_WRITE_ERROR"),
            (277, "INDEX_NOT_USED"),
            (279, "ALL_CONNECTION_TRIES_FAILED"),
            (280, "NO_AVAILABLE_DATA"),
            (281, "DICTIONARY_IS_EMPTY"),
            (282, "INCORRECT_INDEX"),
            (283, "UNKNOWN_DISTRIBUTED_PRODUCT_MODE"),
            (284, "WRONG_GLOBAL_SUBQUERY"),
            (285, "TOO_FEW_LIVE_REPLICAS"),
            (286, "UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE"),
            (287, "UNKNOWN_FORMAT_VERSION"),
            (288, "DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED"),
            (289, "REPLICA_IS_NOT_IN_QUORUM"),
            (290, "LIMIT_EXCEEDED"),
            (291, "DATABASE_ACCESS_DENIED"),
            (293, "MONGODB_CANNOT_AUTHENTICATE"),
            (294, "CANNOT_WRITE_TO_FILE"),
            (295, "RECEIVED_EMPTY_DATA"),
            (297, "SHARD_HAS_NO_CONNECTIONS"),
            (298, "CANNOT_PIPE"),
            (299, "CANNOT_FORK"),
            (300, "CANNOT_DLSYM"),
            (301, "CANNOT_CREATE_CHILD_PROCESS"),
            (302, "CHILD_WAS_NOT_EXITED_NORMALLY"),
            (303, "CANNOT_SELECT"),
            (304, "CANNOT_WAITPID"),
            (305, "TABLE_WAS_NOT_DROPPED"),
            (306, "TOO_DEEP_RECURSION"),
            (307, "TOO_MANY_BYTES"),
            (308, "UNEXPECTED_NODE_IN_ZOOKEEPER"),
            (309, "FUNCTION_CANNOT_HAVE_PARAMETERS"),
            (318, "INVALID_CONFIG_PARAMETER"),
            (319, "UNKNOWN_STATUS_OF_INSERT"),
            (321, "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"),
            (336, "UNKNOWN_DATABASE_ENGINE"),
            (341, "UNFINISHED"),
            (342, "METADATA_MISMATCH"),
            (344, "SUPPORT_IS_DISABLED"),
            (345, "TABLE_DIFFERS_TOO_MUCH"),
            (346, "CANNOT_CONVERT_CHARSET"),
            (347, "CANNOT_LOAD_CONFIG"),
            (349, "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN"),
            (352, "AMBIGUOUS_COLUMN_NAME"),
            (353, "INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE"),
            (354, "ZLIB_INFLATE_FAILED"),
            (355, "ZLIB_DEFLATE_FAILED"),
            (358, "INTO_OUTFILE_NOT_ALLOWED"),
            (359, "TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT"),
            (360, "CANNOT_CREATE_CHARSET_CONVERTER"),
            (361, "SEEK_POSITION_OUT_OF_BOUND"),
            (362, "CURRENT_WRITE_BUFFER_IS_EXHAUSTED"),
            (363, "CANNOT_CREATE_IO_BUFFER"),
            (364, "RECEIVED_ERROR_TOO_MANY_REQUESTS"),
            (366, "SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT"),
            (369, "ALL_REPLICAS_ARE_STALE"),
            (370, "DATA_TYPE_CANNOT_BE_USED_IN_TABLES"),
            (371, "INCONSISTENT_CLUSTER_DEFINITION"),
            (372, "SESSION_NOT_FOUND"),
            (373, "SESSION_IS_LOCKED"),
            (374, "INVALID_SESSION_TIMEOUT"),
            (375, "CANNOT_DLOPEN"),
            (376, "CANNOT_PARSE_UUID"),
            (377, "ILLEGAL_SYNTAX_FOR_DATA_TYPE"),
            (378, "DATA_TYPE_CANNOT_HAVE_ARGUMENTS"),
            (380, "CANNOT_KILL"),
            (381, "HTTP_LENGTH_REQUIRED"),
            (382, "CANNOT_LOAD_CATBOOST_MODEL"),
            (383, "CANNOT_APPLY_CATBOOST_MODEL"),
            (384, "PART_IS_TEMPORARILY_LOCKED"),
            (385, "MULTIPLE_STREAMS_REQUIRED"),
            (386, "NO_COMMON_TYPE"),
            (387, "DICTIONARY_ALREADY_EXISTS"),
            (388, "CANNOT_ASSIGN_OPTIMIZE"),
            (389, "INSERT_WAS_DEDUPLICATED"),
            (390, "CANNOT_GET_CREATE_TABLE_QUERY"),
            (391, "EXTERNAL_LIBRARY_ERROR"),
            (392, "QUERY_IS_PROHIBITED"),
            (393, "THERE_IS_NO_QUERY"),
            (394, "QUERY_WAS_CANCELLED"),
            (395, "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"),
            (396, "TOO_MANY_ROWS_OR_BYTES"),
            (397, "QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW"),
            (398, "UNKNOWN_MUTATION_COMMAND"),
            (399, "FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT"),
            (400, "CANNOT_STAT"),
            (401, "FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME"),
            (402, "CANNOT_IOSETUP"),
            (403, "INVALID_JOIN_ON_EXPRESSION"),
            (404, "BAD_ODBC_CONNECTION_STRING"),
            (406, "TOP_AND_LIMIT_TOGETHER"),
            (407, "DECIMAL_OVERFLOW"),
            (408, "BAD_REQUEST_PARAMETER"),
            (410, "EXTERNAL_SERVER_IS_NOT_RESPONDING"),
            (411, "PTHREAD_ERROR"),
            (412, "NETLINK_ERROR"),
            (413, "CANNOT_SET_SIGNAL_HANDLER"),
            (415, "ALL_REPLICAS_LOST"),
            (416, "REPLICA_STATUS_CHANGED"),
            (417, "EXPECTED_ALL_OR_ANY"),
            (418, "UNKNOWN_JOIN"),
            (419, "MULTIPLE_ASSIGNMENTS_TO_COLUMN"),
            (420, "CANNOT_UPDATE_COLUMN"),
            (421, "CANNOT_ADD_DIFFERENT_AGGREGATE_STATES"),
            (422, "UNSUPPORTED_URI_SCHEME"),
            (423, "CANNOT_GETTIMEOFDAY"),
            (424, "CANNOT_LINK"),
            (425, "SYSTEM_ERROR"),
            (427, "CANNOT_COMPILE_REGEXP"),
            (429, "FAILED_TO_GETPWUID"),
            (430, "MISMATCHING_USERS_FOR_PROCESS_AND_DATA"),
            (431, "ILLEGAL_SYNTAX_FOR_CODEC_TYPE"),
            (432, "UNKNOWN_CODEC"),
            (433, "ILLEGAL_CODEC_PARAMETER"),
            (434, "CANNOT_PARSE_PROTOBUF_SCHEMA"),
            (435, "NO_COLUMN_SERIALIZED_TO_REQUIRED_PROTOBUF_FIELD"),
            (436, "PROTOBUF_BAD_CAST"),
            (437, "PROTOBUF_FIELD_NOT_REPEATED"),
            (438, "DATA_TYPE_CANNOT_BE_PROMOTED"),
            (439, "CANNOT_SCHEDULE_TASK"),
            (440, "INVALID_LIMIT_EXPRESSION"),
            (441, "CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING"),
            (442, "BAD_DATABASE_FOR_TEMPORARY_TABLE"),
            (443, "NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS"),
            (444, "UNKNOWN_PROTOBUF_FORMAT"),
            (445, "CANNOT_MPROTECT"),
            (446, "FUNCTION_NOT_ALLOWED"),
            (447, "HYPERSCAN_CANNOT_SCAN_TEXT"),
            (448, "BROTLI_READ_FAILED"),
            (449, "BROTLI_WRITE_FAILED"),
            (450, "BAD_TTL_EXPRESSION"),
            (451, "BAD_TTL_FILE"),
            (452, "SETTING_CONSTRAINT_VIOLATION"),
            (453, "MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES"),
            (454, "OPENSSL_ERROR"),
            (455, "SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY"),
            (456, "UNKNOWN_QUERY_PARAMETER"),
            (457, "BAD_QUERY_PARAMETER"),
            (458, "CANNOT_UNLINK"),
            (459, "CANNOT_SET_THREAD_PRIORITY"),
            (460, "CANNOT_CREATE_TIMER"),
            (461, "CANNOT_SET_TIMER_PERIOD"),
            (463, "CANNOT_FCNTL"),
            (464, "CANNOT_PARSE_ELF"),
            (465, "CANNOT_PARSE_DWARF"),
            (466, "INSECURE_PATH"),
            (467, "CANNOT_PARSE_BOOL"),
            (468, "CANNOT_PTHREAD_ATTR"),
            (469, "VIOLATED_CONSTRAINT"),
            (471, "INVALID_SETTING_VALUE"),
            (472, "READONLY_SETTING"),
            (473, "DEADLOCK_AVOIDED"),
            (474, "INVALID_TEMPLATE_FORMAT"),
            (475, "INVALID_WITH_FILL_EXPRESSION"),
            (476, "WITH_TIES_WITHOUT_ORDER_BY"),
            (477, "INVALID_USAGE_OF_INPUT"),
            (478, "UNKNOWN_POLICY"),
            (479, "UNKNOWN_DISK"),
            (480, "UNKNOWN_PROTOCOL"),
            (481, "PATH_ACCESS_DENIED"),
            (482, "DICTIONARY_ACCESS_DENIED"),
            (483, "TOO_MANY_REDIRECTS"),
            (484, "INTERNAL_REDIS_ERROR"),
            (487, "CANNOT_GET_CREATE_DICTIONARY_QUERY"),
            (489, "INCORRECT_DICTIONARY_DEFINITION"),
            (490, "CANNOT_FORMAT_DATETIME"),
            (491, "UNACCEPTABLE_URL"),
            (492, "ACCESS_ENTITY_NOT_FOUND"),
            (493, "ACCESS_ENTITY_ALREADY_EXISTS"),
            (495, "ACCESS_STORAGE_READONLY"),
            (496, "QUOTA_REQUIRES_CLIENT_KEY"),
            (497, "ACCESS_DENIED"),
            (498, "LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED"),
            (499, "S3_ERROR"),
            (500, "AZURE_BLOB_STORAGE_ERROR"),
            (501, "CANNOT_CREATE_DATABASE"),
            (502, "CANNOT_SIGQUEUE"),
            (503, "AGGREGATE_FUNCTION_THROW"),
            (504, "FILE_ALREADY_EXISTS"),
            (507, "UNABLE_TO_SKIP_UNUSED_SHARDS"),
            (508, "UNKNOWN_ACCESS_TYPE"),
            (509, "INVALID_GRANT"),
            (510, "CACHE_DICTIONARY_UPDATE_FAIL"),
            (511, "UNKNOWN_ROLE"),
            (512, "SET_NON_GRANTED_ROLE"),
            (513, "UNKNOWN_PART_TYPE"),
            (514, "ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND"),
            (515, "INCORRECT_ACCESS_ENTITY_DEFINITION"),
            (516, "AUTHENTICATION_FAILED"),
            (517, "CANNOT_ASSIGN_ALTER"),
            (518, "CANNOT_COMMIT_OFFSET"),
            (519, "NO_REMOTE_SHARD_AVAILABLE"),
            (520, "CANNOT_DETACH_DICTIONARY_AS_TABLE"),
            (521, "ATOMIC_RENAME_FAIL"),
            (523, "UNKNOWN_ROW_POLICY"),
            (524, "ALTER_OF_COLUMN_IS_FORBIDDEN"),
            (525, "INCORRECT_DISK_INDEX"),
            (527, "NO_SUITABLE_FUNCTION_IMPLEMENTATION"),
            (528, "CASSANDRA_INTERNAL_ERROR"),
            (529, "NOT_A_LEADER"),
            (530, "CANNOT_CONNECT_RABBITMQ"),
            (531, "CANNOT_FSTAT"),
            (532, "LDAP_ERROR"),
            (535, "UNKNOWN_RAID_TYPE"),
            (536, "CANNOT_RESTORE_FROM_FIELD_DUMP"),
            (537, "ILLEGAL_MYSQL_VARIABLE"),
            (538, "MYSQL_SYNTAX_ERROR"),
            (539, "CANNOT_BIND_RABBITMQ_EXCHANGE"),
            (540, "CANNOT_DECLARE_RABBITMQ_EXCHANGE"),
            (541, "CANNOT_CREATE_RABBITMQ_QUEUE_BINDING"),
            (542, "CANNOT_REMOVE_RABBITMQ_EXCHANGE"),
            (543, "UNKNOWN_MYSQL_DATATYPES_SUPPORT_LEVEL"),
            (544, "ROW_AND_ROWS_TOGETHER"),
            (545, "FIRST_AND_NEXT_TOGETHER"),
            (546, "NO_ROW_DELIMITER"),
            (547, "INVALID_RAID_TYPE"),
            (548, "UNKNOWN_VOLUME"),
            (549, "DATA_TYPE_CANNOT_BE_USED_IN_KEY"),
            (552, "UNRECOGNIZED_ARGUMENTS"),
            (553, "LZMA_STREAM_ENCODER_FAILED"),
            (554, "LZMA_STREAM_DECODER_FAILED"),
            (555, "ROCKSDB_ERROR"),
            (556, "SYNC_MYSQL_USER_ACCESS_ERROR"),
            (557, "UNKNOWN_UNION"),
            (558, "EXPECTED_ALL_OR_DISTINCT"),
            (559, "INVALID_GRPC_QUERY_INFO"),
            (560, "ZSTD_ENCODER_FAILED"),
            (561, "ZSTD_DECODER_FAILED"),
            (562, "TLD_LIST_NOT_FOUND"),
            (563, "CANNOT_READ_MAP_FROM_TEXT"),
            (564, "INTERSERVER_SCHEME_DOESNT_MATCH"),
            (565, "TOO_MANY_PARTITIONS"),
            (566, "CANNOT_RMDIR"),
            (567, "DUPLICATED_PART_UUIDS"),
            (568, "RAFT_ERROR"),
            (569, "MULTIPLE_COLUMNS_SERIALIZED_TO_SAME_PROTOBUF_FIELD"),
            (570, "DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD"),
            (571, "DATABASE_REPLICATION_FAILED"),
            (572, "TOO_MANY_QUERY_PLAN_OPTIMIZATIONS"),
            (573, "EPOLL_ERROR"),
            (574, "DISTRIBUTED_TOO_MANY_PENDING_BYTES"),
            (575, "UNKNOWN_SNAPSHOT"),
            (576, "KERBEROS_ERROR"),
            (577, "INVALID_SHARD_ID"),
            (578, "INVALID_FORMAT_INSERT_QUERY_WITH_DATA"),
            (579, "INCORRECT_PART_TYPE"),
            (580, "CANNOT_SET_ROUNDING_MODE"),
            (581, "TOO_LARGE_DISTRIBUTED_DEPTH"),
            (582, "NO_SUCH_PROJECTION_IN_TABLE"),
            (583, "ILLEGAL_PROJECTION"),
            (584, "PROJECTION_NOT_USED"),
            (585, "CANNOT_PARSE_YAML"),
            (586, "CANNOT_CREATE_FILE"),
            (587, "CONCURRENT_ACCESS_NOT_SUPPORTED"),
            (588, "DISTRIBUTED_BROKEN_BATCH_INFO"),
            (589, "DISTRIBUTED_BROKEN_BATCH_FILES"),
            (590, "CANNOT_SYSCONF"),
            (591, "SQLITE_ENGINE_ERROR"),
            (592, "DATA_ENCRYPTION_ERROR"),
            (593, "ZERO_COPY_REPLICATION_ERROR"),
            (594, "BZIP2_STREAM_DECODER_FAILED"),
            (595, "BZIP2_STREAM_ENCODER_FAILED"),
            (596, "INTERSECT_OR_EXCEPT_RESULT_STRUCTURES_MISMATCH"),
            (597, "NO_SUCH_ERROR_CODE"),
            (598, "BACKUP_ALREADY_EXISTS"),
            (599, "BACKUP_NOT_FOUND"),
            (600, "BACKUP_VERSION_NOT_SUPPORTED"),
            (601, "BACKUP_DAMAGED"),
            (602, "NO_BASE_BACKUP"),
            (603, "WRONG_BASE_BACKUP"),
            (604, "BACKUP_ENTRY_ALREADY_EXISTS"),
            (605, "BACKUP_ENTRY_NOT_FOUND"),
            (606, "BACKUP_IS_EMPTY"),
            (607, "CANNOT_RESTORE_DATABASE"),
            (608, "CANNOT_RESTORE_TABLE"),
            (609, "FUNCTION_ALREADY_EXISTS"),
            (610, "CANNOT_DROP_FUNCTION"),
            (611, "CANNOT_CREATE_RECURSIVE_FUNCTION"),
            (614, "POSTGRESQL_CONNECTION_FAILURE"),
            (615, "CANNOT_ADVISE"),
            (616, "UNKNOWN_READ_METHOD"),
            (617, "LZ4_ENCODER_FAILED"),
            (618, "LZ4_DECODER_FAILED"),
            (619, "POSTGRESQL_REPLICATION_INTERNAL_ERROR"),
            (620, "QUERY_NOT_ALLOWED"),
            (621, "CANNOT_NORMALIZE_STRING"),
            (622, "CANNOT_PARSE_CAPN_PROTO_SCHEMA"),
            (623, "CAPN_PROTO_BAD_CAST"),
            (624, "BAD_FILE_TYPE"),
            (625, "IO_SETUP_ERROR"),
            (626, "CANNOT_SKIP_UNKNOWN_FIELD"),
            (627, "BACKUP_ENGINE_NOT_FOUND"),
            (628, "OFFSET_FETCH_WITHOUT_ORDER_BY"),
            (629, "HTTP_RANGE_NOT_SATISFIABLE"),
            (630, "HAVE_DEPENDENT_OBJECTS"),
            (631, "UNKNOWN_FILE_SIZE"),
            (632, "UNEXPECTED_DATA_AFTER_PARSED_VALUE"),
            (633, "QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW"),
            (634, "MONGODB_ERROR"),
            (635, "CANNOT_POLL"),
            (636, "CANNOT_EXTRACT_TABLE_STRUCTURE"),
            (637, "INVALID_TABLE_OVERRIDE"),
            (638, "SNAPPY_UNCOMPRESS_FAILED"),
            (639, "SNAPPY_COMPRESS_FAILED"),
            (640, "NO_HIVEMETASTORE"),
            (641, "CANNOT_APPEND_TO_FILE"),
            (642, "CANNOT_PACK_ARCHIVE"),
            (643, "CANNOT_UNPACK_ARCHIVE"),
            (645, "NUMBER_OF_DIMENSIONS_MISMATCHED"),
            (647, "CANNOT_BACKUP_TABLE"),
            (648, "WRONG_DDL_RENAMING_SETTINGS"),
            (649, "INVALID_TRANSACTION"),
            (650, "SERIALIZATION_ERROR"),
            (651, "CAPN_PROTO_BAD_TYPE"),
            (652, "ONLY_NULLS_WHILE_READING_SCHEMA"),
            (653, "CANNOT_PARSE_BACKUP_SETTINGS"),
            (654, "WRONG_BACKUP_SETTINGS"),
            (655, "FAILED_TO_SYNC_BACKUP_OR_RESTORE"),
            (659, "UNKNOWN_STATUS_OF_TRANSACTION"),
            (660, "HDFS_ERROR"),
            (661, "CANNOT_SEND_SIGNAL"),
            (662, "FS_METADATA_ERROR"),
            (663, "INCONSISTENT_METADATA_FOR_BACKUP"),
            (664, "ACCESS_STORAGE_DOESNT_ALLOW_BACKUP"),
            (665, "CANNOT_CONNECT_NATS"),
            (667, "NOT_INITIALIZED"),
            (668, "INVALID_STATE"),
            (669, "NAMED_COLLECTION_DOESNT_EXIST"),
            (670, "NAMED_COLLECTION_ALREADY_EXISTS"),
            (671, "NAMED_COLLECTION_IS_IMMUTABLE"),
            (672, "INVALID_SCHEDULER_NODE"),
            (673, "RESOURCE_ACCESS_DENIED"),
            (674, "RESOURCE_NOT_FOUND"),
            (675, "CANNOT_PARSE_IPV4"),
            (676, "CANNOT_PARSE_IPV6"),
            (677, "THREAD_WAS_CANCELED"),
            (678, "IO_URING_INIT_FAILED"),
            (679, "IO_URING_SUBMIT_ERROR"),
            (690, "MIXED_ACCESS_PARAMETER_TYPES"),
            (691, "UNKNOWN_ELEMENT_OF_ENUM"),
            (692, "TOO_MANY_MUTATIONS"),
            (693, "AWS_ERROR"),
            (694, "ASYNC_LOAD_CYCLE"),
            (695, "ASYNC_LOAD_FAILED"),
            (696, "ASYNC_LOAD_CANCELED"),
            (697, "CANNOT_RESTORE_TO_NONENCRYPTED_DISK"),
            (698, "INVALID_REDIS_STORAGE_TYPE"),
            (699, "INVALID_REDIS_TABLE_STRUCTURE"),
            (700, "USER_SESSION_LIMIT_EXCEEDED"),
            (701, "CLUSTER_DOESNT_EXIST"),
            (702, "CLIENT_INFO_DOES_NOT_MATCH"),
            (703, "INVALID_IDENTIFIER"),
            (704, "QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS"),
            (705, "TABLE_NOT_EMPTY"),
            (706, "LIBSSH_ERROR"),
            (707, "GCP_ERROR"),
            (708, "ILLEGAL_STATISTICS"),
            (709, "CANNOT_GET_REPLICATED_DATABASE_SNAPSHOT"),
            (710, "FAULT_INJECTED"),
            (711, "FILECACHE_ACCESS_DENIED"),
            (712, "TOO_MANY_MATERIALIZED_VIEWS"),
            (713, "BROKEN_PROJECTION"),
            (714, "UNEXPECTED_CLUSTER"),
            (715, "CANNOT_DETECT_FORMAT"),
            (716, "CANNOT_FORGET_PARTITION"),
            (717, "EXPERIMENTAL_FEATURE_ERROR"),
            (718, "TOO_SLOW_PARSING"),
            (719, "QUERY_CACHE_USED_WITH_SYSTEM_TABLE"),
            (720, "USER_EXPIRED"),
            (721, "DEPRECATED_FUNCTION"),
            (722, "ASYNC_LOAD_WAIT_FAILED"),
            (723, "PARQUET_EXCEPTION"),
            (724, "TOO_MANY_TABLES"),
            (725, "TOO_MANY_DATABASES"),
            (726, "UNEXPECTED_HTTP_HEADERS"),
            (727, "UNEXPECTED_TABLE_ENGINE"),
            (728, "UNEXPECTED_DATA_TYPE"),
            (729, "ILLEGAL_TIME_SERIES_TAGS"),
            (730, "REFRESH_FAILED"),
            (731, "QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE"),
            (733, "TABLE_IS_BEING_RESTARTED"),
            (734, "CANNOT_WRITE_AFTER_BUFFER_CANCELED"),
            (735, "QUERY_WAS_CANCELLED_BY_CLIENT"),
            (736, "DATALAKE_DATABASE_ERROR"),
            (737, "GOOGLE_CLOUD_ERROR"),
            (738, "PART_IS_LOCKED"),
            (739, "BUZZHOUSE"),
            (740, "POTENTIALLY_BROKEN_DATA_PART"),
            (741, "TABLE_UUID_MISMATCH"),
            (742, "DELTA_KERNEL_ERROR"),
            (743, "ICEBERG_SPECIFICATION_VIOLATION"),
            (744, "SESSION_ID_EMPTY"),
            (745, "SERVER_OVERLOADED"),
            (900, "DISTRIBUTED_CACHE_ERROR"),
            (901, "CANNOT_USE_DISTRIBUTED_CACHE"),
            (902, "PROTOCOL_VERSION_MISMATCH"),
            (903, "LICENSE_EXPIRED"),
            (999, "KEEPER_EXCEPTION"),
            (1000, "POCO_EXCEPTION"),
            (1001, "STD_EXCEPTION"),
            (1002, "UNKNOWN_EXCEPTION"),
            (1003, "SSH_EXCEPTION"),
        ])
    });
