#![doc = include_str!("../README.md")]

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
#[cfg(feature = "test-utils")]
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
