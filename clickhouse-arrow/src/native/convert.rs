use std::borrow::Cow;

use crate::{Error, Result, Type, Value};

pub mod raw_row;
pub mod std_deserialize;
pub mod std_serialize;
pub use raw_row::*;
pub mod unit_value;

/// Type alias for the definition of a column for schema creation
pub type ColumnDefinition<T = Value> = (String, Type, Option<T>);

/// A type that can be converted to a raw `ClickHouse` SQL value.
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

/// A type that can be converted from a raw `ClickHouse` SQL value.
pub trait FromSql: Sized {
    /// # Errors
    fn from_sql(type_: &Type, value: Value) -> Result<Self>;
}

impl FromSql for Value {
    fn from_sql(_type_: &Type, value: Value) -> Result<Self> { Ok(value) }
}

/// A row that can be deserialized and serialized from a raw `ClickHouse` SQL value.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_definition_alias() {
        let col_def: ColumnDefinition = ("test".to_string(), Type::Int32, Some(Value::Int32(42)));
        assert_eq!(col_def.0, "test");
        assert_eq!(col_def.1, Type::Int32);
        assert_eq!(col_def.2, Some(Value::Int32(42)));
    }

    #[test]
    fn test_column_definition_generic() {
        let col_def: ColumnDefinition<i32> = ("test".to_string(), Type::Int32, Some(42));
        assert_eq!(col_def.0, "test");
        assert_eq!(col_def.1, Type::Int32);
        assert_eq!(col_def.2, Some(42));
    }

    #[test]
    fn test_value_to_sql() {
        let value = Value::Int32(42);
        let result = value.to_sql(None).unwrap();
        assert_eq!(result, Value::Int32(42));
    }

    #[test]
    fn test_value_to_sql_with_hint() {
        let value = Value::String("test".to_string().into_bytes());
        let result = value.to_sql(Some(&Type::String)).unwrap();
        assert_eq!(result, Value::String("test".to_string().into_bytes()));
    }

    #[test]
    fn test_value_from_sql() {
        let type_ = Type::Int32;
        let value = Value::Int32(123);
        let result = Value::from_sql(&type_, value).unwrap();
        assert_eq!(result, Value::Int32(123));
    }

    #[test]
    fn test_unexpected_type() {
        let type_ = Type::Int32;
        let error = unexpected_type(&type_);

        match error {
            Error::DeserializeError(msg) => {
                assert!(msg.contains("unexpected type"));
                assert!(msg.contains("Int32"));
            }
            _ => panic!("Expected DeserializeError"),
        }
    }

    #[test]
    fn test_unexpected_type_different_types() {
        let types =
            vec![Type::String, Type::Int64, Type::Float32, Type::Array(Box::new(Type::Int32))];

        for type_ in types {
            let error = unexpected_type(&type_);
            assert!(matches!(error, Error::DeserializeError(_)));
        }
    }

    // Test that the module exports work correctly
    #[test]
    fn test_module_exports() {
        // Test that we can use exported types
        let _raw_row = RawRow::default();

        // Test that we can use the type alias
        let _col_def: ColumnDefinition = ("test".to_string(), Type::Int32, None);
    }
}
