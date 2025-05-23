#![doc = include_str!("../README.md")]

pub mod arrow;
mod client;
mod client_info;
mod compression;
mod constants;
mod ddl;
mod errors;
mod flags;
mod formats;
mod io;
pub mod native;
#[cfg(feature = "pool")]
mod pool;
pub mod prelude;
mod query;
mod settings;
pub mod spawn;
pub mod telemetry;

#[cfg(feature = "derive")]
/// Derive macro for the [Row] trait.
///
/// This is similar in usage and implementation to the [`serde::Serialize`] and
/// [`serde::Deserialize`] derive macros.
///
/// ## serde attributes
/// The following [serde attributes](https://serde.rs/attributes.html) are supported, using `#[clickhouse_native(...)]` instead of `#[serde(...)]`:
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
/// ## Clickhouse-specific attributes
/// - The `nested` attribute allows handling [Clickhouse nested data structures](https://clickhouse.com/docs/en/sql-reference/data-types/nested-data-structures/nested).
///   See an example in the `tests` folder.
///
/// ## Known issues
/// - For serialization, the ordering of fields in the struct declaration must match the order in the `INSERT` statement, respectively in the table declaration. See issue [#34](https://github.com/Protryon/clickhouse_native/issues/34).
pub use clickhouse_native_derive::Row;
pub use client::*;
/// Set this environment to enable additional debugs around arrow (de)serialization.
pub use constants::DEBUG_ARROW_ENV_VAR;
pub use ddl::CreateOptions;
pub use errors::*;
pub use io::*;
/// Contains useful top-level traits to interface with [`crate::prelude::NativeFormat`]
pub use native::convert::*;
pub use native::progress::Progress;
pub use native::protocol::ProfileEvent;
/// Represents the types that `ClickHouse` supports internally.
pub use native::types::*;
/// Contains useful top-level structures to interface with [`crate::prelude::NativeFormat`]
pub use native::values::*;
pub use native::{CompressionMethod, ServerError, Severity};
#[cfg(feature = "pool")]
pub use pool::*;
pub use query::{ParsedQuery, Qid};
/// Re-exports
///
/// Exporting different external modules used by the library.
pub use reexports::*;
pub use settings::Settings;
mod reexports {
    #[cfg(feature = "pool")]
    pub use bb8;
    pub use chrono_tz::Tz;
    pub use indexmap::IndexMap;
    pub use uuid::Uuid;
    pub use {rustc_hash, tracing};
}

// Type aliases used throughout the library
pub use aliases::*;
mod aliases {
    /// A non-cryptographically secure [`std::hash::BuildHasherDefault`] using
    /// [`rustc_hash::FxHasher`].
    pub type HashBuilder = std::hash::BuildHasherDefault<rustc_hash::FxHasher>;
    /// A non-cryptographically secure [`indexmap::IndexMap`] using [`HashBuilder`].
    pub type FxIndexMap<K, V> = indexmap::IndexMap<K, V, HashBuilder>;
}

// Silent lints for dev dependencies
#[cfg(test)]
mod dev_crates {
    use {clickhouse as _, criterion as _, testcontainers as _, tracing_subscriber as _};
}
