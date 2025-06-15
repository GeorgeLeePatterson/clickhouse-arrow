use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::CompressionMethod;

// ClickHouse headers
pub(crate) const X_CLICKHOUSE_USER: &str = "X-ClickHouse-User";
pub(crate) const X_CLICKHOUSE_KEY: &str = "X-ClickHouse-Key";
pub(crate) const X_CLICKHOUSE_FORMAT: &str = "X-ClickHouse-Format";
pub(crate) const X_CLICKHOUSE_SUMMARY: &str = "X-ClickHouse-Summary";

// ClickHouse default sizes
pub(crate) const CLICKHOUSE_DEFAULT_CHUNK_BYTES: usize = 523_272;
#[expect(unused)]
pub(crate) const CLICKHOUSE_DEFAULT_CHUNK_ROWS: usize = 65_409;

/// Default settings for `RowBinary` format
pub(crate) const DEFAULT_SETTINGS: &[(&str, &str)] = &[
    // Deserialization currently assumes this
    ("output_format_binary_write_json_as_string", "1"),
];

/// Returned in headers after querying, with compression included, allowing override of compression
/// method.
///
/// Currently `RowBinary` supports zstd compression only at the network level. `CompressionMethod`
/// tracks compression at the block level, which means compression here must be distinguished from
/// network level compression.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HttpSummary {
    #[serde(deserialize_with = "de_string_to_usize")]
    pub(crate) read_rows:          usize,
    #[serde(deserialize_with = "de_string_to_usize")]
    pub(crate) read_bytes:         usize,
    #[serde(deserialize_with = "de_string_to_usize")]
    pub(crate) total_rows_to_read: usize,
    #[serde(default, deserialize_with = "de_string_to_option_usize")]
    pub(crate) result_rows:        Option<usize>,
    #[serde(default, deserialize_with = "de_string_to_option_usize")]
    pub(crate) result_bytes:       Option<usize>,
    #[serde(deserialize_with = "de_string_to_usize")]
    pub(crate) elapsed_ns:         usize,
    // Block level compression, not network level
    #[serde(default)]
    pub(crate) compression:        CompressionMethod,
}

impl HttpSummary {
    pub(crate) fn with_compression(mut self, compression: CompressionMethod) -> Self {
        self.compression = compression;
        self
    }
}

impl FromStr for HttpSummary {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> { serde_json::from_str::<HttpSummary>(s) }
}

/// Deserializes a stringified number (e.g., "9000") into usize. `ClickHouse` provides the header as
/// string values, not numbers
fn de_string_to_usize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<usize, D::Error> {
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::String(s) => s.parse().map_err(serde::de::Error::custom),
        Value::Number(n) => n
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| serde::de::Error::custom("Invalid number for usize")),
        _ => Err(serde::de::Error::custom("Expected string or number")),
    }
}

/// Deserializes a stringified number (e.g., "9000") into usize. `ClickHouse` provides the header as
/// string values, not numbers
/// Deserializes a stringified number (e.g., "9000"), direct number (e.g., 9000), or null into
/// Option<usize>.
fn de_string_to_option_usize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::String(s) => s.parse::<usize>().map(Some).map_err(serde::de::Error::custom),
        Value::Number(n) => n
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom("Invalid number for usize")),
        Value::Null => Ok(None),
        _ => Err(serde::de::Error::custom("Expected string, number, or null")),
    }
}

#[test]
fn test_http_summary_deserialize() {
    // String format (current)
    let json_str = r#"{
        "read_rows":"9000",
        "read_bytes":"72000",
        "total_rows_to_read":"9000",
        "result_rows":"9000",
        "result_bytes":"72128",
        "elapsed_ns":"11034834"
    }"#;
    let summary_str: HttpSummary = serde_json::from_str(json_str).unwrap();
    // Number format (future)
    let json_num = r#"{
        "read_rows":9000,
        "read_bytes":72000,
        "total_rows_to_read":9000,
        "result_rows":9000,
        "result_bytes":72128,
        "elapsed_ns":11034834
    }"#;
    let summary_num: HttpSummary = serde_json::from_str(json_num).unwrap();
    let expected = HttpSummary {
        read_rows:          9000,
        read_bytes:         72000,
        total_rows_to_read: 9000,
        result_rows:        Some(9000),
        result_bytes:       Some(72128),
        elapsed_ns:         11_034_834,
        compression:        CompressionMethod::default(),
    };
    assert_eq!(summary_str, expected);
    assert_eq!(summary_num, expected);
    // String format (opt)
    let json_str = r#"{
           "read_rows":"9000",
           "read_bytes":"72000",
           "total_rows_to_read":"9000",
           "elapsed_ns":"11034834"
       }"#;
    let summary_str: HttpSummary = serde_json::from_str(json_str).unwrap();
    // Number format (opt)
    let json_num = r#"{
           "read_rows":9000,
           "read_bytes":72000,
           "total_rows_to_read":9000,
           "elapsed_ns":11034834
       }"#;
    let summary_num: HttpSummary = serde_json::from_str(json_num).unwrap();
    let expected = HttpSummary {
        read_rows:          9000,
        read_bytes:         72000,
        total_rows_to_read: 9000,
        result_rows:        None,
        result_bytes:       None,
        elapsed_ns:         11_034_834,
        compression:        CompressionMethod::default(),
    };
    assert_eq!(summary_str, expected);
    assert_eq!(summary_num, expected);

    // Invalid format
    let json_invalid = r#"{
        "read_rows":"invalid",
        "read_bytes":72000,
        "total_rows_to_read":9000,
        "result_rows":9000,
        "result_bytes":72128,
        "elapsed_ns":11034834
    }"#;
    assert!(serde_json::from_str::<HttpSummary>(json_invalid).is_err());
}
