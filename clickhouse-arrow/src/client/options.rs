use std::path::PathBuf;

use tracing::warn;

use super::CompressionMethod;
use crate::native::protocol::ChunkedProtocolMode;
use crate::prelude::Secret;

/// Configuration options for a `ClickHouse` client connection and Arrow serialization.
///
/// The `ClientOptions` struct defines the settings used to establish a connection
/// to a `ClickHouse` server and handle data serialization/deserialization with
/// Apache Arrow. These options are typically set via [`super::builder::ClientBuilder`] methods
/// (e.g., [`super::builder::ClientBuilder::with_username`],
/// [`super::builder::ClientBuilder::with_tls`]) or constructed directly for use with
/// [`crate::Client::connect`].
///
/// # Fields
/// - `username`: The username for authenticating with `ClickHouse` (default: `"default"`).
/// - `password`: The password for authentication, stored securely as a [`Secret`].
/// - `default_database`: The default database for queries; if empty, uses `ClickHouse`'s
///   `"default"` database.
/// - `domain`: Optional domain for TLS verification; inferred from the destination if unset.
/// - `ipv4_only`: If `true`, restricts address resolution to IPv4; if `false`, allows IPv6.
/// - `cafile`: Optional path to a certificate authority file for TLS connections.
/// - `use_tls`: If `true`, enables TLS for secure connections; if `false`, uses plain TCP.
/// - `compression`: The compression method for data exchange (default: [`CompressionMethod::LZ4`]).
/// - `arrow`: Optional Arrow-specific serialization options (see [`ArrowOptions`]).
/// - `cloud`: Cloud-specific options for `ClickHouse` cloud instances (requires `cloud` feature).
///
/// # Examples
/// ```rust,ignore
/// use clickhouse_arrow::prelude::*;
///
/// let options = ClientOptions {
///     username: "admin".to_string(),
///     password: Secret::new("secret"),
///     default_database: "my_db".to_string(),
///     use_tls: true,
///     ..ClientOptions::default()
/// };
///
/// let client = Client::connect("localhost:9000", options, None, None)
///     .await
///     .unwrap();
/// ```
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClientOptions {
    /// Username credential
    pub username:         String,
    /// Password credential. [`Secret`] is used to minimize likelihood of exposure through logs
    pub password:         Secret,
    /// Scope this client to a specifc database, otherwise 'default' is used
    pub default_database: String,
    /// For tls, provide the domain, otherwise it will be determined from the endpoint.
    pub domain:           Option<String>,
    /// Whether any non-ipv4 socket addrs should be filtered out.
    pub ipv4_only:        bool,
    /// Provide a path to a certificate authority to use for tls.
    pub cafile:           Option<PathBuf>,
    /// Whether a connection should be made securely over tls.
    pub use_tls:          bool,
    /// The compression to use when sending data to clickhouse.
    pub compression:      CompressionMethod,
    /// Additional configuration not core to `ClickHouse` connections
    #[cfg_attr(feature = "serde", serde(default))]
    pub ext:              Extension,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            username:         "default".to_string(),
            password:         Secret::new(""),
            default_database: String::new(),
            domain:           None,
            ipv4_only:        false,
            cafile:           None,
            use_tls:          false,
            compression:      CompressionMethod::default(),
            ext:              Extension::default(),
        }
    }
}

impl ClientOptions {
    /// Create a new `ClientOptions` with default values.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    #[must_use]
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = username.into();
        self
    }

    #[must_use]
    pub fn with_password(mut self, password: impl Into<Secret>) -> Self {
        self.password = password.into();
        self
    }

    #[must_use]
    pub fn with_default_database(mut self, default_database: impl Into<String>) -> Self {
        self.default_database = default_database.into();
        self
    }

    #[must_use]
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    #[must_use]
    pub fn with_ipv4_only(mut self, ipv4_only: bool) -> Self {
        self.ipv4_only = ipv4_only;
        self
    }

    #[must_use]
    pub fn with_cafile<P: AsRef<std::path::Path>>(mut self, cafile: P) -> Self {
        self.cafile = Some(cafile.as_ref().into());
        self
    }

    #[must_use]
    pub fn with_use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    #[must_use]
    pub fn with_compression(mut self, compression: CompressionMethod) -> Self {
        self.compression = compression;
        self
    }

    #[must_use]
    pub fn with_extension(mut self, ext: Extension) -> Self {
        self.ext = ext;
        self
    }

    #[must_use]
    pub fn extend(mut self, ext: impl Fn(Extension) -> Extension) -> Self {
        self.ext = ext(self.ext);
        self
    }
}

/// Extra configuration options for `ClickHouse`.
///
/// These options are separated to allow extending the configuration capabilities of a connection
/// without breaking the core [`ClientOptions`] that are unlikely to ever change. For this reason,
/// `Extensions` is `non_exhaustive` so the api can change without breaking existing
/// implementations.
#[non_exhaustive]
#[derive(Debug, Default, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Extension {
    /// Options specific to (de)serializing arrow data.
    pub arrow:          Option<ArrowOptions>,
    /// Options specific to communicating with `ClickHouse` over their cloud offering.
    #[cfg(feature = "cloud")]
    pub cloud:          CloudOptions,
    /// Options related to server/client protocol send chunking.
    /// This may be removed, as it may be defaulted.
    #[cfg_attr(feature = "serde", serde(default))]
    pub chunked_send:   ChunkedProtocolMode,
    /// Options related to server/client protocol recv chunking.
    /// This may be removed, as it may be defaulted
    #[cfg_attr(feature = "serde", serde(default))]
    pub chunked_recv:   ChunkedProtocolMode,
    /// Related to `inner_pool`, how many 'inner clients' to spawn. Currently capped at 4.
    #[cfg(feature = "inner_pool")]
    #[cfg_attr(feature = "serde", serde(default))]
    pub fast_mode_size: Option<u8>,
}

/// Configuration extensions for specialized `ClickHouse` client behavior.
///
/// This type provides additional configuration options beyond the standard
/// client settings, including Arrow format options and cloud-specific settings.
impl Extension {
    #[must_use]
    pub fn with_arrow(mut self, options: ArrowOptions) -> Self {
        self.arrow = Some(options);
        self
    }

    #[must_use]
    pub fn with_set_arrow(mut self, f: impl Fn(ArrowOptions) -> ArrowOptions) -> Self {
        self.arrow = Some(f(self.arrow.unwrap_or_default()));
        self
    }

    #[must_use]
    pub fn with_chunked_send_mode(mut self, mode: ChunkedProtocolMode) -> Self {
        self.chunked_send = mode;
        self
    }

    #[must_use]
    pub fn with_chunked_recv_mode(mut self, mode: ChunkedProtocolMode) -> Self {
        self.chunked_recv = mode;
        self
    }

    #[cfg(feature = "cloud")]
    #[must_use]
    pub fn with_cloud(mut self, options: CloudOptions) -> Self {
        self.cloud = options;
        self
    }

    #[cfg(feature = "inner_pool")]
    #[must_use]
    pub fn with_fast_mode_size(mut self, size: u8) -> Self {
        self.fast_mode_size = Some(size);
        self
    }
}

// TODO: Remove - make the properties public!
/// Configuration options for Arrow serialization and deserialization with `ClickHouse`.
///
/// The `ArrowOptions` struct defines settings that control how Apache Arrow data types
/// are mapped to `ClickHouse` types during serialization (e.g., inserts), deserialization
/// (e.g., queries), and schema creation (e.g., DDL operations). These options are used
/// by [`crate::ArrowClient`] and set via [`super::builder::ClientBuilder::with_arrow_options`] or
/// directly in [`ClientOptions`].
///
/// # Fields
/// - `strings_as_strings`: If `true`, maps `ClickHouse` `String` to Arrow `Utf8`; if `false`, maps
///   to `Binary` (default).
/// - `use_date32_for_date`: If `true`, maps Arrow `Date32` to `ClickHouse` `Date32`; if `false`,
///   maps to `Date` (default).
/// - `strict_schema`: If `true`, enforces strict type mappings during serialization (inserts) and
///   schema creation, causing errors on `ClickHouse` invariant violations (e.g.,
///   `Nullable(LowCardinality(String))`); if `false`, attempts to correct violations (e.g., mapping
///   to `LowCardinality(Nullable(String))`) (default).
/// - `disable_strict_schema_ddl`: If `true`, prevents automatic strict mode during schema creation
///   (via [`ArrowOptions::into_strict_ddl`]); if `false`, schema creation defaults to strict mode
///   (default).
/// - `nullable_array_default_empty`: If `true`, maps `Nullable(Array(...))` to `Array(...)` with
///   `[]` for nulls during inserts and schema creation (if `disable_strict_schema_ddl = true`); if
///   `false`, errors on `Nullable(Array(...))` (default).
///
/// # Notes
/// - During schema creation, options are converted to strict mode (via
///   [`ArrowOptions::into_strict_ddl`]) unless `disable_strict_schema_ddl` is `true`. Strict mode
///   sets `strict_schema = true` and effectively enforces `nullable_array_default_empty = false`,
///   ensuring non-nullable arrays.
/// - When `strict_schema` is `false`, violations like `Nullable(LowCardinality(String))` are
///   corrected, but arrays are handled per `nullable_array_default_empty` for inserts, while
///   nullable arrays are ignored during schema creation.
/// - If `strict_schema = true` and `nullable_array_default_empty = true`, non-array violations
///   (e.g., `LowCardinality`) error, but arrays map to `[]` for nulls during insert. This is useful
///   in cases where the arrow `Schema` is used to create the table, but arrays may come from
///   different `RecordBatch`es.
/// - This struct is `#[non_exhaustive]`, so future fields may be added (e.g., for new `ClickHouse`
///   types or serialization options). Use [`ArrowOptions::new`] or [`ArrowOptions::default`] to
///   construct instances.
///
/// # Examples
/// ```rust,ignore
/// use clickhouse_arrow::prelude::*;
///
/// let arrow_options = ArrowOptions::new()
///     .with_strings_as_strings(true)
///     .with_strict_schema(true)
///     .with_nullable_array_default_empty(false);
/// let options = ClientOptions {
///     arrow: Some(arrow_options),
///     ..ClientOptions::default()
/// };
/// ```
#[expect(clippy::struct_excessive_bools)]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ArrowOptions {
    pub strings_as_strings:           bool,
    pub use_date32_for_date:          bool,
    pub strict_schema:                bool,
    pub disable_strict_schema_ddl:    bool,
    pub nullable_array_default_empty: bool,
}

impl Default for ArrowOptions {
    /// Creates an `ArrowOptions` instance with default values.
    ///
    /// The default configuration uses relaxed type mappings suitable for most
    /// `ClickHouse` and `Arrow` use cases:
    /// - `ClickHouse` `String` maps to Arrow `Binary`.
    /// - Arrow `Date32` maps to `ClickHouse` `Date`.
    /// - Type mappings are relaxed, correcting `ClickHouse` invariant violations (e.g., mapping
    ///   `Nullable(LowCardinality(String))` to `LowCardinality(Nullable(String))`).
    /// - Schema creation defaults to strict mode (via [`ArrowOptions::into_strict_ddl`]).
    /// - `Nullable(Array(...))` defaults to `Array(...)` with `[]` for nulls.
    ///
    /// Use this as a starting point and customize with methods like
    /// [`ArrowOptions::with_strings_as_strings`].
    ///
    /// # Returns
    /// An [`ArrowOptions`] instance with default settings.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let arrow_options = ArrowOptions::default();
    /// println!("Nullable array default empty: {}", arrow_options.nullable_array_default_empty); // true
    /// ```
    fn default() -> Self { Self::new() }
}

impl ArrowOptions {
    /// Creates a new `ArrowOptions` instance with default values.
    ///
    /// This method is equivalent to [`ArrowOptions::default`], initializing fields for
    /// relaxed type mappings. Use this to start configuring Arrow
    /// serialization/deserialization options for `ClickHouse`.
    ///
    /// # Returns
    /// A new [`ArrowOptions`] instance with default settings.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let arrow_options = ArrowOptions::new();
    /// println!("Nullable array default empty: {}", arrow_options.nullable_array_default_empty); // true
    /// ```
    pub const fn new() -> Self {
        Self {
            strings_as_strings:           false,
            use_date32_for_date:          false,
            strict_schema:                false,
            disable_strict_schema_ddl:    false,
            nullable_array_default_empty: true,
        }
    }

    /// Creates an `ArrowOptions` instance with strict type mapping settings.
    ///
    /// This method configures options for strict type mappings, where `ClickHouse`
    /// invariant violations (e.g., `Nullable(LowCardinality(String))` or
    /// `Nullable(Array(...))`) cause errors during serialization (inserts) and schema
    /// creation. It sets `strict_schema` to `true` and `nullable_array_default_empty` to
    /// `false`, leaving other fields as `false`. Use this for operations where
    /// `ClickHouse` invariants must be strictly enforced.
    ///
    /// # Returns
    /// An [`ArrowOptions`] instance with strict settings.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let arrow_options = ArrowOptions::strict();
    /// println!("Strict schema: {}", arrow_options.strict_schema); // true
    /// ```
    pub const fn strict() -> Self {
        Self {
            strings_as_strings:           false,
            use_date32_for_date:          false,
            strict_schema:                true,
            disable_strict_schema_ddl:    false,
            nullable_array_default_empty: false,
        }
    }

    /// Converts the options to strict mode for schema creation, unless disabled.
    ///
    /// This method returns a new [`ArrowOptions`] with strict settings (equivalent to
    /// [`ArrowOptions::strict`]) unless `disable_strict_schema_ddl` is `true`. If
    /// `disable_strict_schema_ddl` is `true`, the original options are returned
    /// unchanged. This method is called automatically during schema creation to enforce
    /// `ClickHouse` invariants, including non-nullable arrays, unless explicitly disabled.
    ///
    /// # Returns
    /// A new [`ArrowOptions`] instance with strict settings or the original options.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let options_strict_off = ArrowOptions::new()
    ///     .with_disable_strict_schema_ddl(true)
    ///     .into_strict_ddl();
    /// assert!(!options_strict_off.strict_schema);
    /// assert!(options_strict_off.nullable_array_default_empty);
    ///
    /// let options_strict = ArrowOptions::new()
    ///     .with_disable_strict_schema_ddl(false) // Default
    ///     .into_strict_ddl();
    /// assert!(options_strict.strict_schema);
    /// assert!(!options_strict.nullable_array_default_empty);
    /// ```
    #[must_use]
    pub fn into_strict_ddl(self) -> Self {
        if self.disable_strict_schema_ddl {
            return self;
        }

        Self {
            strings_as_strings: self.strings_as_strings,
            use_date32_for_date: self.use_date32_for_date,
            ..Self::strict()
        }
    }

    /// Sets whether `ClickHouse` `String` types are deserialized as Arrow `Utf8`.
    ///
    /// By default, `ClickHouse` `String` types map to Arrow `Binary`. When this option
    /// is enabled (`true`), they map to Arrow `Utf8`, which is more suitable for text
    /// data. Use this to control serialization/deserialization behavior for string
    /// columns.
    ///
    /// # Parameters
    /// - `enabled`: If `true`, maps [`crate::Type::String`] to
    ///   [`arrow::datatypes::DataType::Utf8`]; if `false`, maps to
    ///   [`arrow::datatypes::DataType::Binary`].
    ///
    /// # Returns
    /// A new [`ArrowOptions`] with the updated setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let arrow_options = ArrowOptions::new()
    ///     .with_strings_as_strings(true);
    /// println!("Strings as strings: {}", arrow_options.strings_as_strings); // true
    /// ```
    #[must_use]
    pub fn with_strings_as_strings(mut self, enabled: bool) -> Self {
        self.strings_as_strings = enabled;
        self
    }

    /// Sets whether Arrow `Date32` is mapped to `ClickHouse` `Date` or `Date32`.
    ///
    /// By default, Arrow `Date32` maps to `ClickHouse` `Date` (days since 1970-01-01).
    /// When this option is enabled (`true`), it maps to `ClickHouse` `Date32` (days
    /// since 1900-01-01). Use this to control date serialization/deserialization
    /// behavior.
    ///
    /// # Parameters
    /// - `enabled`: If `true`, maps `Date32` to `ClickHouse` `Date32`; if `false`, maps to `Date`.
    ///
    /// # Returns
    /// A new [`ArrowOptions`] with the updated setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let arrow_options = ArrowOptions::new()
    ///     .with_use_date32_for_date(true);
    /// println!("Use Date32 for Date: {}", arrow_options.use_date32_for_date); // true
    /// ```
    #[must_use]
    pub fn with_use_date32_for_date(mut self, enabled: bool) -> Self {
        self.use_date32_for_date = enabled;
        self
    }

    /// Sets whether type mappings are strict during serialization and schema creation.
    ///
    /// By default, type mappings are relaxed, allowing `ClickHouse` invariant violations
    /// (e.g., `Nullable(LowCardinality(String))`) to be corrected automatically (e.g.,
    /// mapping to `LowCardinality(Nullable(String))`). When this option is enabled
    /// (`true`), non-array violations cause errors during serialization (inserts) and
    /// schema creation. Array violations are controlled by
    /// [`ArrowOptions::with_nullable_array_default_empty`]. Schema creation defaults to
    /// strict mode unless [`ArrowOptions::with_disable_strict_schema_ddl`] is enabled.
    ///
    /// # Parameters
    /// - `enabled`: If `true`, enforces strict type mappings for non-array types; if `false`,
    ///   allows relaxed corrections.
    ///
    /// # Returns
    /// A new [`ArrowOptions`] with the updated setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let arrow_options = ArrowOptions::new()
    ///     .with_strict_schema(true);
    /// println!("Strict schema: {}", arrow_options.strict_schema); // true
    /// ```
    #[must_use]
    pub fn with_strict_schema(mut self, enabled: bool) -> Self {
        self.strict_schema = enabled;
        self
    }

    /// Sets whether strict mode is disabled during schema creation.
    ///
    /// By default, schema creation (e.g., DDL operations) uses strict type mappings (via
    /// [`ArrowOptions::into_strict_ddl`]), enforcing `ClickHouse` invariants and causing
    /// errors on violations, including `Nullable(Array(...))`. When this option is
    /// enabled (`true`), strict mode is disabled for schema creation, using the user’s
    /// `strict_schema` and `nullable_array_default_empty` settings.
    ///
    /// # Parameters
    /// - `enabled`: If `true`, disables strict mode for schema creation; if `false`, enables strict
    ///   mode.
    ///
    /// # Returns
    /// A new [`ArrowOptions`] with the updated setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let arrow_options = ArrowOptions::new()
    ///     .with_disable_strict_schema_ddl(true);
    /// assert!(arrow_options.disable_strict_schema_ddl);
    /// ```
    #[must_use]
    pub fn with_disable_strict_schema_ddl(mut self, enabled: bool) -> Self {
        self.disable_strict_schema_ddl = enabled;
        self
    }

    /// Sets whether `Nullable(Array(...))` types default to empty arrays during inserts and are
    /// coerced to non-nullable during DDL.
    ///
    /// By default, `Nullable(Array(...))` types are mapped to `Array(...)` with `[]` for
    /// nulls during serialization (inserts) and schema creation (if
    /// `disable_strict_schema_ddl = true`). When this option is disabled (`false`),
    /// `Nullable(Array(...))` causes errors, enforcing non-nullable arrays. Schema
    /// creation defaults to non-nullable arrays unless
    /// [`ArrowOptions::with_disable_strict_schema_ddl`] is enabled.
    ///
    /// # Parameters
    /// - `enabled`: If `true`, maps `Nullable(Array(...))` to `Array(...)` with `[]` for nulls; if
    ///   `false`, errors on `Nullable(Array(...))`.
    ///
    /// # Returns
    /// A new [`ArrowOptions`] with the updated setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let arrow_options = ArrowOptions::new()
    ///     .with_nullable_array_default_empty(false);
    /// assert!(!arrow_options.nullable_array_default_empty);
    /// ```
    #[must_use]
    pub fn with_nullable_array_default_empty(mut self, enabled: bool) -> Self {
        self.nullable_array_default_empty = enabled;
        self
    }

    /// Sets an Arrow option by name and value.
    ///
    /// This method updates a specific option identified by `name` to the given boolean
    /// `value`. Currently supported names are:
    /// - `"strings_as_strings"`: Maps `ClickHouse` `String` to Arrow `Utf8`.
    /// - `"use_date32_for_date"`: Maps Arrow `Date32` to `ClickHouse` `Date32`.
    /// - `"strict_schema"`: Enforces strict type mappings for non-array types.
    /// - `"disable_strict_schema_ddl"`: Disables strict mode for schema creation.
    /// - `"nullable_array_default_empty"`: Maps `Nullable(Array(...))` to `Array(...)` with `[]`
    ///   for nulls.
    ///
    /// If an unrecognized name is provided, a warning is logged, and the options are
    /// returned unchanged. Use this for dynamic configuration or when options are
    /// specified as key-value pairs.
    ///
    /// # Parameters
    /// - `name`: The name of the option to set.
    /// - `value`: The boolean value to set for the option.
    ///
    /// # Returns
    /// A new [`ArrowOptions`] with the updated setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let arrow_options = ArrowOptions::new()
    ///     .with_setting("strings_as_strings", true)
    ///     .with_setting("nullable_array_default_empty", false);
    /// assert!(arrow_options.strings_as_strings);
    /// assert!(!arrow_options.nullable_array_default_empty);
    /// ```
    #[must_use]
    pub fn with_setting(self, name: &str, value: bool) -> Self {
        match name {
            "strings_as_strings" => self.with_strings_as_strings(value),
            "use_date32_for_date" => self.with_use_date32_for_date(value),
            "strict_schema" => self.with_strict_schema(value),
            "disable_strict_schema_ddl" => self.with_disable_strict_schema_ddl(value),
            "nullable_array_default_empty" => self.with_nullable_array_default_empty(value),
            k => {
                warn!("Unrecognized option for ArrowOptions: {k}");
                self
            }
        }
    }
}

impl<'a, S, I> From<I> for ArrowOptions
where
    S: AsRef<str> + 'a,
    I: Iterator<Item = &'a (S, bool)> + 'a,
{
    /// Creates an `ArrowOptions` instance from an iterator of key-value pairs.
    ///
    /// This method constructs an [`ArrowOptions`] by applying settings from an iterator
    /// of `(key, value)` pairs, where `key` is a string (e.g., `"strict_schema"`) and
    /// `value` is a boolean. It uses [`ArrowOptions::with_setting`] to apply each
    /// setting. Unrecognized keys trigger a warning but do not cause an error.
    ///
    /// See currently supported keys by inspecting [`ArrowOptions::with_setting`].
    ///
    /// # Parameters
    /// - `value`: An iterator of `(key, value)` pairs, where `key` is a string-like type and
    ///   `value` is a boolean.
    ///
    /// # Returns
    /// An [`ArrowOptions`] instance with the applied settings.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let settings = vec![("strings_as_strings", true), ("nullable_array_default_empty", false)];
    /// let arrow_options: ArrowOptions = settings.iter().collect();
    /// assert!(arrow_options.strings_as_strings);
    /// assert!(!arrow_options.nullable_array_default_empty);
    /// ```
    fn from(value: I) -> Self {
        let mut options = ArrowOptions::default();
        for (k, v) in value {
            options = options.with_setting(k.as_ref(), *v);
        }
        options
    }
}

impl<'a> FromIterator<(&'a str, bool)> for ArrowOptions {
    /// Creates an `ArrowOptions` instance from an iterator of string-boolean pairs.
    ///
    /// This method constructs an [`ArrowOptions`] by applying settings from an iterator
    /// of `(key, value)` pairs, where `key` is a string slice (e.g., `"strict_schema"`)
    /// and `value` is a boolean. It uses [`ArrowOptions::with_setting`] to apply each
    /// setting. Unrecognized keys trigger a warning but do not cause an error.
    ///
    /// See currently supported keys by inspecting [`ArrowOptions::with_setting`].
    ///
    /// # Parameters
    /// - `iter`: An iterator of `(key, value)` pairs, where `key` is a string slice and `value` is
    ///   a boolean.
    ///
    /// # Returns
    /// An [`ArrowOptions`] instance with the applied settings.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::arrow::ArrowOptions;
    ///
    /// let settings = vec![("strings_as_strings", true), ("nullable_array_default_empty", false)];
    /// let arrow_options = ArrowOptions::from_iter(settings);
    /// assert!(arrow_options.strings_as_strings);
    /// assert!(!arrow_options.nullable_array_default_empty);
    /// ```
    fn from_iter<I: IntoIterator<Item = (&'a str, bool)>>(iter: I) -> Self {
        let mut options = ArrowOptions::default();
        for (k, v) in iter {
            options = options.with_setting(k, v);
        }
        options
    }
}

/// Configuration options for connecting to `ClickHouse` cloud instances.
///
/// The `CloudOptions` struct defines settings specific to `ClickHouse` cloud
/// deployments, used within [`ClientOptions`]. These options control the behavior
/// of cloud wakeup pings to ensure the instance is active before connecting.
///
/// # Fields
/// - `timeout`: Optional timeout (in seconds) for the cloud wakeup ping; if `None`, uses a default
///   timeout.
/// - `wakeup`: If `true`, sends a wakeup ping before connecting; if `false`, skips the ping.
///
/// # Feature
/// Requires the `cloud` feature to be enabled.
///
/// # Examples
/// ```rust,ignore
/// use clickhouse_arrow::prelude::*;
///
/// let cloud_options = CloudOptions {
///     timeout: Some(10),
///     wakeup: true,
/// };
/// let options = ClientOptions {
///     cloud: cloud_options,
///     ..ClientOptions::default()
/// };
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CloudOptions {
    #[cfg_attr(feature = "serde", serde(default))]
    pub timeout: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub wakeup:  bool,
}
