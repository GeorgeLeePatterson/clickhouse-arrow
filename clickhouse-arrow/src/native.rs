//! ## Logic for interfacing between internal 'native' types and `ClickHouse`
pub mod block;
pub mod block_info;
pub(crate) mod client_info;
pub mod convert;
pub mod error_codes;
pub mod progress;
pub(crate) mod protocol;
pub(crate) mod sparse;
pub mod types;
pub mod values;

pub use self::error_codes::{ServerError, Severity};
pub use self::protocol::CompressionMethod;
