use std::path::PathBuf;

use tracing::warn;

use super::CompressionMethod;
use crate::prelude::Secret;

/// Options set for a Clickhouse connection and arrow (de)serialization.
#[derive(Debug, Clone, PartialEq)]
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
    /// Options specific to (de)serializing arrow data.
    pub arrow:            Option<ArrowOptions>,
    /// Options specific to communicating with `ClickHouse` over their cloud offering.
    #[cfg(feature = "cloud")]
    pub cloud:            CloudOptions,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            username:                        "default".to_string(),
            password:                        Secret::new(""),
            default_database:                String::new(),
            domain:                          None,
            ipv4_only:                       false,
            cafile:                          None,
            use_tls:                         false,
            compression:                     CompressionMethod::default(),
            arrow:                           None,
            #[cfg(feature = "cloud")]
            cloud:                           CloudOptions::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CloudOptions {
    pub timeout: Option<u64>,
    pub wakeup:  bool,
}

/// Various options used during arrow to `ClickHouse` serialization and deserialization.
///
/// # Fields
/// - `strings_as_strings`: Whether `Type::String` will be interpreted as `Type::Binary`
/// - `use_date32_for_date`: Whether `DataType::Date32` will map to `Type::Date` (the default) or to
///   `Type::Date32` (which offsets from a different start, ie 1900-01-01).
/// - `array_nullable_error`: Whether `Type::Nullable(Type::Array)` will throw an error
/// - `low_cardinality_nullable_error`: Whether `Type::Nullable(Type::LowCardinality)` will throw an
///   error
/// - `strict_schema_conversion`: Inserts will by default handle schema mismatches (such as that
///   which `array_nullable_error` catches). Schema creation does not and uses a strict schema with
///   these options set to `false`. This reverses that so the schema can still be created but the
///   violations of `ClickHouse` invariants will be corrected. This can cause unexpected outcomes if
///   the schema used to create the database is the source of truth.
#[expect(clippy::struct_excessive_bools)]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArrowOptions {
    pub(crate) strings_as_strings:             bool,
    pub(crate) use_date32_for_date:            bool,
    pub(crate) array_nullable_error:           bool,
    pub(crate) low_cardinality_nullable_error: bool,
    pub(crate) strict_schema_conversion:       bool,
}

/// Helpful methods for [`ArrowOptions`].
impl ArrowOptions {
    pub const fn new() -> Self {
        Self {
            strings_as_strings:             false,
            use_date32_for_date:            false,
            array_nullable_error:           false,
            low_cardinality_nullable_error: false,
            strict_schema_conversion:       false,
        }
    }

    /// Construct a strict set of arrow options for schema creation
    ///
    /// Strict corresponds to throwing errors on invalid types
    pub const fn strict() -> Self {
        Self {
            strings_as_strings:             false,
            use_date32_for_date:            false,
            array_nullable_error:           true,
            low_cardinality_nullable_error: true,
            strict_schema_conversion:       false,
        }
    }

    /// Converts the options into schema strict mode UNLESS `strict_schema_conversion` is enabled.
    #[must_use]
    pub fn into_strict(self) -> Self {
        if self.strict_schema_conversion {
            return self;
        }

        Self::strict()
    }

    /// Set whether [`crate::Type::String`] should be deserialized as
    /// [`arrow::datatypes::DataType::Utf8`] or the default
    /// [`arrow::datatypes::DataType::Binary`]
    #[must_use]
    pub fn with_strings_as_strings(mut self, enabled: bool) -> Self {
        self.strings_as_strings = enabled;
        self
    }

    /// Set whether [`arrow::datatypes::DataType::Date32`] should be (de)serialized as
    /// [`crate::Type::Date`] (the default) or [`crate::Type::Date32`]
    #[must_use]
    pub fn with_use_date32_for_date(mut self, enabled: bool) -> Self {
        self.use_date32_for_date = enabled;
        self
    }

    /// Set whether Nullable(Array(...)) should cause an error.
    #[must_use]
    pub fn with_array_nullable_error(mut self, enabled: bool) -> Self {
        self.array_nullable_error = enabled;
        self
    }

    /// Set whether Nullable(LowCardinality(...)) should cause an error.
    #[must_use]
    pub fn with_low_cardinality_nullable_error(mut self, enabled: bool) -> Self {
        self.low_cardinality_nullable_error = enabled;
        self
    }

    /// Set whether schema creation normalizes types to prevent errors
    #[must_use]
    pub fn with_strict_schema_conversion(mut self, enabled: bool) -> Self {
        self.strict_schema_conversion = enabled;
        self
    }

    #[must_use]
    pub fn with_setting(self, name: &str, value: bool) -> Self {
        match name {
            "strings_as_strings" => self.with_strings_as_strings(value),
            "use_date32_for_date" => self.with_use_date32_for_date(value),
            "array_nullable_error" => self.with_array_nullable_error(value),
            "low_cardinality_nullable_error" => self.with_low_cardinality_nullable_error(value),
            "strict_schema_conversion" => self.with_strict_schema_conversion(value),
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
    /// Matches against the key value provided and changes the default setting based on the value
    /// provided.
    fn from(value: I) -> Self {
        let mut options = ArrowOptions::default();
        for (k, v) in value {
            options = options.with_setting(k.as_ref(), *v);
        }
        options
    }
}

/// Collects an iterator of string slices into an [`ArrowOptions`]
impl<'a> FromIterator<(&'a str, bool)> for ArrowOptions {
    fn from_iter<I: IntoIterator<Item = (&'a str, bool)>>(iter: I) -> Self {
        let mut options = ArrowOptions::default();
        for (k, v) in iter {
            options = options.with_setting(k, v);
        }
        options
    }
}
