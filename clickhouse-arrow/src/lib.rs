//! # 🛰️ `ClickHouse` *Native Protocol* Rust Client w/ Arrow Compatibility
//!
//! `ClickHouse` access in rust over `ClickHouse`'s native protocol.
//!
//! A high-performance, async Rust client for `ClickHouse` with native Arrow integration. Designed
//! to be faster and more memory-efficient than existing alternatives.
//!
//! ## Why clickhouse-arrow?
//!
//! - **🚀 Performance**: Optimized for speed with zero-copy deserialization where possible
//! - **🎯 Arrow Native**: First-class Apache Arrow support for efficient data interchange
//! - **📊 90%+ Test Coverage**: Comprehensive test suite ensuring reliability
//! - **🔄 Async/Await**: Modern async API built on Tokio
//! - **🗜️ Compression**: LZ4 and ZSTD support for efficient data transfer
//! - **☁️ Cloud Ready**: Full `ClickHouse` Cloud compatibility
//! - **🛡️ Type Safe**: Compile-time type checking with the `#[derive(Row)]` macro
//!
//! ## Details
//!
//! The crate supports two "modes" of operation:
//!
//! ### `ArrowFormat`
//!
//! Support allowing interoperability with [arrow](https://docs.rs/arrow/latest/arrow/).
//!
//! ### `NativeFormat`
//!
//! Uses internal types and custom traits if a dependency on arrow is not required.
//!
//! ### `CreateOptions`, `SchemaConversions`, and Schemas
//!
//! #### Creating Tables from Arrow Schemas
//!
//! `clickhouse-arrow` provides powerful DDL capabilities through `CreateOptions`, allowing you to
//! create `ClickHouse` tables directly from Arrow schemas:
//!
//! ```rust,ignore
//! use clickhouse_arrow::{Client, ArrowFormat, CreateOptions};
//! use arrow::datatypes::{Schema, Field, DataType};
//!
//! // Define your Arrow schema
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::UInt64, false),
//!     Field::new("name", DataType::Utf8, false),
//!     Field::new("status", DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)), false),
//! ]);
//!
//! // Configure table creation
//! let options = CreateOptions::new("MergeTree")
//!     .with_order_by(&["id".to_string()])
//!     .with_partition_by("toYYYYMM(created_at)")
//!     .with_setting("index_granularity", 8192);
//!
//! // Create the table
//! client.create_table(None, "my_table", &schema, &options, None).await?;
//! ```
//!
//! #### Schema Conversions for Type Control
//!
//! `SchemaConversions` (type alias for `HashMap<String, Type>`) provides fine-grained control over
//! Arrow-to-ClickHouse type mappings. This is especially important for:
//!
//! 1. **Converting Dictionary → Enum**: By default, Arrow Dictionary types map to
//!    `LowCardinality(String)`. Use `SchemaConversions` to map them to `Enum8` or `Enum16` instead:
//!
//! ```rust,ignore
//! use clickhouse_arrow::{Type, CreateOptions};
//! use std::collections::HashMap;
//!
//! let schema_conversions = HashMap::from([
//!     // Convert status column from Dictionary to Enum8
//!     ("status".to_string(), Type::Enum8(vec![
//!         ("active".to_string(), 0),
//!         ("inactive".to_string(), 1),
//!         ("pending".to_string(), 2),
//!     ])),
//!     // Convert category to Enum16 for larger enums
//!     ("category".to_string(), Type::Enum16(vec![
//!         ("electronics".to_string(), 0),
//!         ("clothing".to_string(), 1),
//!         // ... up to 65k values
//!     ])),
//! ]);
//!
//! let options = CreateOptions::new("MergeTree")
//!     .with_order_by(&["id".to_string()])
//!     .with_schema_conversions(schema_conversions);
//! ```
//!
//! 2. **Geo Types**: Preserve geographic types during conversion
//! 3. **Date Types**: Choose between `Date` and `Date32`
//! 4. **Custom Type Mappings**: Override any default type conversion
//!
//! #### Field Naming Constants
//!
//! When working with complex Arrow types, use these constants to ensure compatibility:
//!
//! ```rust,ignore
//! use clickhouse_arrow::arrow::types::*;
//!
//! // For List types - inner field is named "item"
//! let list_field = Field::new("data", DataType::List(
//!     Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true))
//! ), true);
//!
//! // For Struct/Tuple types - fields are named "field_0", "field_1", etc.
//! let tuple_fields = vec![
//!     Field::new(format!("{}{}", TUPLE_FIELD_NAME_PREFIX, 0), DataType::Int32, false),
//!     Field::new(format!("{}{}", TUPLE_FIELD_NAME_PREFIX, 1), DataType::Utf8, false),
//! ];
//!
//! // For Map types - uses specific field names
//! let map_type = DataType::Map(
//!     Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(
//!         vec![
//!             Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false),
//!             Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, true),
//!         ].into()
//!     ), false)),
//!     false
//! );
//! ```
//!
//! These constants ensure your Arrow schemas align with `ClickHouse`'s expectations and maintain
//! compatibility with arrow-rs conventions.
//!
//! ## Queries
//!
//! ### Query Settings
//!
//! The `clickhouse_arrow::Settings` type allows configuring `ClickHouse` query settings. You can
//! import it directly:
//!
//! ```rust,ignore
//! use clickhouse_arrow::Settings;
//! // or via prelude
//! use clickhouse_arrow::prelude::*;
//! ```
//!
//! Refer to the settings module documentation for details and examples.
//!
//! ## Arrow Round-Trip
//!
//! There are cases where a round trip may deserialize a different type by schema or array than the
//! schema and array you used to create the table.
//!
//!  will try to maintain an accurate and updated list as they occur. In addition, when possible, I
//! will provide options or other functionality to alter this behavior.
//!
//! #### `(String|Binary)View`/`Large(List|String|Binary)` variations are normalized.
//! - **Behavior**: `ClickHouse` does not make the same distinction between `Utf8`, `Utf8View`, or
//!   `LargeUtf8`. All of these are mapped to either `Type::Binary` (the default, see above) or
//!   `Type::String`
//! - **Option**: None
//! - **Default**: Unsupported
//! - **Impact**: When deserializing from `ClickHouse`, manual modification will be necessary to use
//!   these data types.
//!
//! #### `Utf8` -> `Binary`
//! - **Behavior**: By default, `Type::String`/`DataType::Utf8` will be represented as Binary.
//! - **Option**: `strings_as_strings` (default: `false`).
//! - **Default**: Disabled (`false`).
//! - **Impact**: Set to `true` to strip map `Type::String` -> `DataType::Utf8`. Binary tends to be
//!   more efficient to work with in high throughput scenarios
//!
//! #### Nullable `Array`s
//! - **Behavior**: `ClickHouse` does not allow `Nullable(Array(...))`, but insertion with non-null
//!   data is allowed by default. To modify this behavior, set `array_nullable_error` to `true`.
//! - **Option**: `array_nullable_error` (default: `false`).
//! - **Default**: Disabled (`false`).
//! - **Impact**: Enables flexible insertion but may cause schema mismatches if nulls are present.
//!
//! #### `LowCardinality(Nullable(...))` vs `Nullable(LowCardinality(...))`
//! - **Behavior**: Like arrays mentioned above, `ClickHouse` does not allow nullable low
//!   cardinality. The default behavior is to push down the nullability.
//! - **Option**: `low_cardinality_nullable_error` (default: `false`).
//! - **Default**: Disabled (`false`).
//! - **Impact**: Enables flexible insertion but may cause schema mismatches if nulls are present.
//!
//! #### `Enum8`/`Enum16` vs. `LowCardinality`
//! - **Behavior**: Arrow `Dictionary` types map to `LowCardinality`, but `ClickHouse` `Enum` types
//!   may also map to `Dictionary`, altering the type on round-trip.
//! - **Option**: No options available rather provide hash maps for either `enum_i8` and/or
//!   `enum_i16` for `CreateOptions` during schema creation.
//! - **Impact**: The default behavior will ignore enums when starting from arrow.

#![allow(unused_crate_dependencies)]

pub mod arrow;
mod client;
mod compression;
mod constants;
mod errors;
mod flags;
mod formats;
mod io;
pub mod native;
#[cfg(feature = "pool")]
mod pool;
pub mod prelude;
mod query;
mod schema;
mod settings;
pub mod spawn;
pub mod telemetry;
#[cfg(any(feature = "test-utils", feature = "tmpfs-size"))]
pub mod test_utils;

#[cfg(feature = "derive")]
/// Derive macro for the [Row] trait.
///
/// This is similar in usage and implementation to the [`serde::Serialize`] and
/// [`serde::Deserialize`] derive macros.
///
/// ## serde attributes
/// The following [serde attributes](https://serde.rs/attributes.html) are supported, using `#[clickhouse_arrow(...)]` instead of `#[serde(...)]`:
/// - `with`
/// - `from` and `into`
/// - `try_from`
/// - `skip`
/// - `default`
/// - `deny_unknown_fields`
/// - `rename`
/// - `rename_all`
/// - `serialize_with`, `deserialize_with`
/// - `skip_deserializing`, `skip_serializing`
/// - `flatten`
///    - Index-based matching is disabled (the column names must match exactly).
///    - Due to the current interface of the [Row] trait, performance might not be optimal, as
///      a value map must be reconstitued for each flattened subfield.
///
/// ## ClickHouse-specific attributes
/// - The `nested` attribute allows handling [ClickHouse nested data structures](https://clickhouse.com/docs/en/sql-reference/data-types/nested-data-structures/nested).
///   See an example in the `tests` folder.
///
/// ## Known issues
/// - For serialization, the ordering of fields in the struct declaration must match the order in the `INSERT` statement, respectively in the table declaration. See issue [#34](https://github.com/Protryon/clickhouse_arrow/issues/34).
pub use clickhouse_arrow_derive::Row;
pub use client::*;
/// Set this environment to enable additional debugs around arrow (de)serialization.
pub use constants::{CONN_READ_BUFFER_ENV_VAR, CONN_WRITE_BUFFER_ENV_VAR, DEBUG_ARROW_ENV_VAR};
pub use errors::*;
pub use formats::{ArrowFormat, ClientFormat, NativeFormat};
/// Contains useful top-level traits to interface with [`crate::prelude::NativeFormat`]
pub use native::convert::*;
pub use native::progress::Progress;
pub use native::protocol::{ChunkedProtocolMode, ProfileEvent};
/// Represents the types that `ClickHouse` supports internally.
pub use native::types::*;
/// Contains useful top-level structures to interface with [`crate::prelude::NativeFormat`]
pub use native::values::*;
pub use native::{CompressionMethod, ServerError, Severity};
#[cfg(feature = "pool")]
pub use pool::*;
pub use query::{ParamValue, ParsedQuery, Qid, QueryParams};
pub use schema::CreateOptions;
pub use settings::{Setting, SettingValue, Settings};

mod aliases {
    /// A non-cryptographically secure [`std::hash::BuildHasherDefault`] using
    /// [`rustc_hash::FxHasher`].
    pub type HashBuilder = std::hash::BuildHasherDefault<rustc_hash::FxHasher>;
    /// A non-cryptographically secure [`indexmap::IndexMap`] using [`HashBuilder`].
    pub type FxIndexMap<K, V> = indexmap::IndexMap<K, V, HashBuilder>;
}
// Type aliases used throughout the library
pub use aliases::*;
// External libraries
mod reexports {
    #[cfg(feature = "pool")]
    pub use bb8;
    pub use chrono_tz::Tz;
    pub use indexmap::IndexMap;
    pub use uuid::Uuid;
    pub use {rustc_hash, tracing};
}
/// Re-exports
///
/// Exporting different external modules used by the library.
pub use reexports::*;

#[cfg(test)]
mod dev_deps {
    //! This is here to silence rustc's unused-crate-dependencies warnings.
    //! See tracking issue [#95513](https://github.com/rust-lang/rust/issues/95513).
    use {clickhouse as _, criterion as _};
}
