use std::fmt;

use uuid::Uuid;

use crate::io::ClickHouseWrite;
use crate::prelude::SettingValue;
use crate::{Result, Settings};

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

impl From<QueryParams> for Settings {
    /// Helpful to serialize params when dispatching a query
    fn from(value: QueryParams) -> Settings { value.0.into_iter().collect() }
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
