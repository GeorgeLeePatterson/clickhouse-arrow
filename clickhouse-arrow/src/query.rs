use std::fmt;

use uuid::Uuid;

use crate::Result;
use crate::io::ClickHouseWrite;
use crate::prelude::SettingValue;
use crate::settings::SETTING_FLAG_CUSTOM;

/// An internal representation of a query id, meant to reduce costs when tracing, passing around,
/// and converting to strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Qid(Uuid);

impl Default for Qid {
    fn default() -> Self { Self::new() }
}

impl Qid {
    /// Generate a new `v4` [`Uuid`]
    pub fn new() -> Self { Self(Uuid::new_v4()) }

    /// Take the inner [`Uuid`]
    pub fn into_inner(self) -> Uuid { self.0 }

    // Convert to 32-char hex string, no heap allocation
    pub(crate) async fn write_id<W: ClickHouseWrite>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; 32];
        let hex = self.0.as_simple().encode_lower(&mut buffer);
        writer.write_string(hex).await
    }

    // Helper to calculate a determinstic hash from a qid
    #[cfg_attr(not(feature = "inner_pool"), expect(unused))]
    pub(crate) fn key(self) -> usize {
        self.into_inner().as_bytes().iter().copied().map(usize::from).sum::<usize>()
    }
}

impl<T: Into<Qid>> From<Option<T>> for Qid {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Qid::default(),
        }
    }
}

impl From<Uuid> for Qid {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl fmt::Display for Qid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use as_simple() for 32-char hex, no heap allocation
        write!(f, "{}", self.0.as_simple())
    }
}

/// Type alias to help distinguish settings from params
pub type ParamValue = SettingValue;

/// Represent parameters that can be passed to bind values during queries.
///
/// `ClickHouse` has very specific syntax for how it manages query parameters. Refer to their docs
/// for more information.
///
/// See:
/// [Queries with parameters](https://clickhouse.com/docs/interfaces/cli#cli-queries-with-parameters)
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryParams(pub Vec<(String, ParamValue)>);

impl<T, K, S> From<T> for QueryParams
where
    T: IntoIterator<Item = (K, S)>,
    K: Into<String>,
    ParamValue: From<S>,
{
    fn from(value: T) -> Self {
        Self(value.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

impl<K, S> FromIterator<(K, S)> for QueryParams
where
    K: Into<String>,
    ParamValue: From<S>,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, S)>,
    {
        iter.into_iter().collect()
    }
}

impl QueryParams {
    /// Returns the number of query parameters.
    pub(crate) fn len(&self) -> usize { self.0.len() }

    /// Encodes query parameters to the `ClickHouse` native protocol.
    ///
    /// Parameters are encoded using the Settings wire format with custom flag:
    /// - key (string)
    /// - flags (varuint) - 0x02 (`settingFlagCustom`) for params
    /// - value (string) - encoded as "field dump" format
    ///
    /// Field dump format follows `ClickHouse's` `Field::restoreFromDump`:
    /// - Strings: `'value'` with escaped single quotes
    /// - Numbers: raw numeric string (e.g., "42", "3.14")
    /// - Booleans: "true" or "false"
    ///
    /// See: <https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Field.cpp#L312>
    ///
    /// # Errors
    /// Returns an error if writing to the stream fails.
    pub(crate) async fn encode<W: ClickHouseWrite>(
        &self,
        writer: &mut W,
        _revision: u64,
    ) -> Result<()> {
        // Encode each parameter using Settings wire format
        for (key, value) in &self.0 {
            writer.write_string(key).await?;
            writer.write_var_uint(SETTING_FLAG_CUSTOM).await?;

            // Encode value as field dump
            let field_dump = encode_field_dump(value);
            writer.write_string(&field_dump).await?;
        }
        Ok(())
    }
}

/// Encodes a `SettingValue` as a `ClickHouse` field dump string for query parameters.
///
/// **IMPORTANT**: `ClickHouse's` native protocol only supports **string** parameters!
/// Non-string values are converted to their string representation.
///
/// This is because `ClickHouse` calls `Settings::toNameToNameMap()` on received parameters,
/// which requires all values to be parseable as quoted strings. The `{param:Type}` syntax
/// in queries tells `ClickHouse` how to cast the string parameter to the desired type.
///
/// Field dump format for parameters:
/// - All values are encoded as quoted strings: `'value'`
/// - Single quotes within strings are escaped: `'` -> `\'`
///
/// # Examples
/// ```rust,ignore
/// encode_field_dump(&SettingValue::String("hello"))    // "'hello'"
/// encode_field_dump(&SettingValue::String("it's"))     // "'it\\'s'"
/// encode_field_dump(&SettingValue::Int(42))             // "'42'"
/// encode_field_dump(&SettingValue::Float(3.14))         // "'3.14'"
/// encode_field_dump(&SettingValue::Bool(true))          // "'true'"
/// ```
///
/// See: <https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/TCPHandler.cpp>
fn encode_field_dump(value: &SettingValue) -> String {
    // All parameter values must be strings for toNameToNameMap() to work
    match value {
        SettingValue::String(s) => format!("'{}'", s.replace('\'', "\\'")),
        SettingValue::Int(i) => format!("'{i}'"),
        SettingValue::Float(f) => format!("'{f}'"),
        SettingValue::Bool(b) => format!("'{b}'"),
    }
}

/// Represents a parsed query.
///
/// In the future this will enable better validation of queries, possibly
/// saving a roundtrip to the database.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ParsedQuery(pub(crate) String);

impl std::ops::Deref for ParsedQuery {
    type Target = String;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl fmt::Display for ParsedQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

impl From<String> for ParsedQuery {
    fn from(q: String) -> ParsedQuery { ParsedQuery(q.trim().to_string()) }
}

impl From<&str> for ParsedQuery {
    fn from(q: &str) -> ParsedQuery { ParsedQuery(q.trim().to_string()) }
}

impl From<&String> for ParsedQuery {
    fn from(q: &String) -> ParsedQuery { ParsedQuery(q.trim().to_string()) }
}
