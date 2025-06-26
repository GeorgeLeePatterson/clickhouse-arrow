use std::borrow::Cow;

use crate::{Error, Result, Type, Value};

pub mod raw_row;
pub mod std_deserialize;
pub mod std_serialize;
pub use raw_row::*;
pub mod unit_value;

/// Type alias for the definition of a column for schema creation
pub type ColumnDefinition<T = Value> = (String, Type, Option<T>);

/// A type that can be converted to a raw ClickHouse SQL value.
pub trait ToSql {
    /// # Errors
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value>;
}

impl ToSql for Value {
    fn to_sql(self, _type_hint_: Option<&Type>) -> Result<Value> { Ok(self) }
}

pub fn unexpected_type(type_: &Type) -> Error {
    Error::DeserializeError(format!("unexpected type: {type_}"))
}

/// A type that can be converted from a raw ClickHouse SQL value.
pub trait FromSql: Sized {
    /// # Errors
    fn from_sql(type_: &Type, value: Value) -> Result<Self>;
}

impl FromSql for Value {
    fn from_sql(_type_: &Type, value: Value) -> Result<Self> { Ok(value) }
}

/// A row that can be deserialized and serialized from a raw ClickHouse SQL value.
/// Generally this is not implemented manually, but using `clickhouse_arrow_derive::Row`,
/// i.e. `#[derive(clickhouse_arrow::Row)]`.
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::Row;
/// #[derive(Row)]
/// struct MyRow {
///     id: String,
///     name: String,
///     age: u8
/// }
/// ```
pub trait Row: Sized {
    /// If `Some`, `serialize_row` and `deserialize_row` MUST return this number of columns
    const COLUMN_COUNT: Option<usize>;

    /// If `Some`, `serialize_row` and `deserialize_row` MUST have these names
    fn column_names() -> Option<Vec<Cow<'static, str>>>;

    /// Infers the schema and returns it.
    fn to_schema() -> Option<Vec<ColumnDefinition<Value>>>;

    /// # Errors
    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self>;

    /// # Errors
    fn serialize_row(
        self,
        type_hints: &[(String, Type)],
    ) -> Result<Vec<(Cow<'static, str>, Value)>>;
}
