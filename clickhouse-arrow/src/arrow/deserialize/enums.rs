//! Deserialization logic for `ClickHouse` `Enum8` and `Enum16` types into Arrow
//! `DictionaryArray`.
//!
//! This module provides a function to deserialize `ClickHouse`’s native format for `Enum8` and
//! `Enum16` types into an Arrow `DictionaryArray` with integer keys (`Int8` or `Int16`) and
//! string values.
//!
//! The `deserialize` function reads raw indices (`i8` for `Enum8`, `i16` for `Enum16`,
//! little-endian) from the reader and maps them to string values based on the provided enum
//! pairs (e.g., `[("a", 1), ("b", 2)]`). Indices are normalized to zero-based keys for Arrow’s
//! `DictionaryArray` (e.g., `1, 2` becomes `0, 1`).
//!
//! # Examples
//! ```rust,ignore
//! use arrow::array::{ArrayRef, DictionaryArray, Int8Array, StringArray};
//! use arrow::datatypes::Int8Type;
//! use clickhouse_arrow::types::{Type, enums::deserialize, DeserializerState};
//! use std::sync::Arc;
//! use tokio::io::Cursor;
//!
//! #[tokio::test]
//! async fn test_enum8() {
//!     let pairs = vec![("a".to_string(), 1_i8), ("b".to_string(), 2_i8)];
//!     let data = vec![1, 2, 1]; // Keys: [1, 2, 1] -> ["a", "b", "a"]
//!     let mut reader = Cursor::new(data);
//!
//!     let array = deserialize(&Type::Enum8(pairs), &mut reader, 3, &[])
//!         .await
//!         .unwrap();
//!     let keys = Arc::new(Int8Array::from(vec![0, 1, 0])) as ArrayRef; // Normalized keys
//!     let values = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
//!     let expected =
//!         Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap()) as ArrayRef;
//!     assert_eq!(array.as_ref(), expected.as_ref());
//! }
//! ```
use std::sync::Arc;

use arrow::array::*;
use tokio::io::AsyncReadExt;

use crate::arrow::builder::TypedBuilder;
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

/// Deserializes a `ClickHouse` `Enum8` or `Enum16` type into an Arrow `DictionaryArray`.
///
/// The implementation iterates over the `pairs` vector for index lookup, optimized for small enum
/// sizes (typically <100 elements), avoiding `HashMap` allocations and ensuring cache-friendly
/// access. The output `DictionaryArray` contains string values, with original index mappings
/// preserved in the `Type::Enum` metadata for serialization or schema queries. For example, an
/// input of `[1, 2, 1]` for `Enum8` with `pairs = [("a", 1), ("b", 2)]` produces a
/// `DictionaryArray` with keys `[0, 1, 0]` and values `["a", "b"]`, representing `["a", "b", "a"]`.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` `Type` (`Enum8` or `Enum16`) indicating the target type.
/// - `reader`: The async reader providing the `ClickHouse` native format data (raw `i8` or `i16`
///   indices).
/// - `rows`: The number of rows to deserialize.
/// - `nulls`: A slice indicating null values (`1` for null, `0` for non-null).
/// - `state`: A mutable `DeserializerState` for deserialization context (unused).
///
/// # Returns
/// A `Result` containing the deserialized `DictionaryArray` as an `ArrayRef` or a
/// `Error` if deserialization fails.
///
/// # Errors
/// - Returns `ArrowDeserialize` if:
///   - The `type_hint` is not `Enum8` or `Enum16`.
///   - An index is invalid (not found in `pairs`).
///   - The `DictionaryArray` construction fails (e.g., due to mismatched key/value lengths).
/// - Returns `Io` if reading from the reader fails.
///
/// # Performance
/// The implementation is optimized for high-throughput deserialization:
/// - Uses a single `Vec<Option<i8>>` or `Vec<Option<i16>>` allocation for keys, sized to `rows`.
/// - Iterates over `pairs` (small, typically <100 elements) for index lookup, cache-friendly and
///   fast.
/// - Avoids `HashMap` or deduplication overhead, minimizing allocations and memory fragmentation.
/// - Constructs the `StringArray` for values directly from `pairs`, with minimal copying.
pub(super) async fn deserialize_async<R: ClickHouseRead>(
    type_hint: &Type,
    builder: &mut TypedBuilder,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
) -> Result<ArrayRef> {
    super::deser!(() => builder => {
        TypedBuilder::Enum8(b) => {{
            let Type::Enum8(pairs) = type_hint else {
                return Err(Error::UnexpectedType(type_hint.clone()));
            };
            for i in 0..rows {
                let idx = super::primitive::primitive_async!(Int8 => reader);
                if nulls.is_empty() || nulls[i] == 0 {
                    // Find index in pairs
                    b.append_value(&pairs.iter().find(|(_, key)| *key == idx).ok_or(
                        Error::ArrowDeserialize(format!(
                            "Invalid Enum8 index: {idx} not found in pairs"
                        ))
                    )?.0);
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }},
        TypedBuilder::Enum16(b) => {{
            let Type::Enum16(pairs) = type_hint else {
                return Err(Error::UnexpectedType(type_hint.clone()));
            };
            for i in 0..rows {
                let idx = super::primitive::primitive_async!(Int16 => reader);
                if nulls.is_empty() || nulls[i] == 0 {
                    // Find index in pairs
                    b.append_value(&pairs.iter().find(|(_, key)| *key == idx).ok_or(
                        Error::ArrowDeserialize(format!(
                            "Invalid Enum16 index: {idx} not found in pairs"
                        ))
                    )?.0);
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }}
    }
    _ => { Err(Error::ArrowDeserialize(format!(
        "Unexpected builder type for enum: {type_hint:?}"
    )))})
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{DictionaryArray, Int8Array, Int16Array, StringArray};
    use arrow::datatypes::{Int8Type, Int16Type};

    use super::*;

    // Helper to create a mock reader
    type MockReader = Cursor<Vec<u8>>;

    #[tokio::test]
    async fn test_deserialize_enum8() {
        let pairs = vec![("a".to_string(), 1_i8), ("b".to_string(), 2_i8)];
        let data = vec![1, 2, 1]; // Keys: [1, 2, 1] -> ["a", "b", "a"]
        let mut reader = MockReader::new(data);

        let type_ = Type::Enum8(pairs);
        let data_type = arrow::datatypes::DataType::Dictionary(
            Box::new(arrow::datatypes::DataType::Int8),
            Box::new(arrow::datatypes::DataType::Utf8),
        );
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = deserialize_async(&type_, &mut builder, &mut reader, 3, &[]).await.unwrap();
        let values = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let expected = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1, 0]), values).unwrap(),
        ) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    #[tokio::test]
    async fn test_deserialize_enum16() {
        let pairs = vec![("x".to_string(), 10_i16), ("y".to_string(), 20_i16)];
        let data = vec![10, 0, 20, 0, 10, 0]; // Keys: [10, 20, 10] -> ["x", "y", "x"] in LE
        let mut reader = MockReader::new(data);

        let type_ = Type::Enum16(pairs);
        let data_type = arrow::datatypes::DataType::Dictionary(
            Box::new(arrow::datatypes::DataType::Int16),
            Box::new(arrow::datatypes::DataType::Utf8),
        );
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = deserialize_async(&type_, &mut builder, &mut reader, 3, &[]).await.unwrap();
        let values = Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef;
        let expected = Arc::new(
            DictionaryArray::<Int16Type>::try_new(Int16Array::from(vec![0, 1, 0]), values).unwrap(),
        ) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    #[tokio::test]
    async fn test_deserialize_enum8_nullable() {
        let pairs = vec![("a".to_string(), 1_i8), ("b".to_string(), 2_i8)];
        let data = vec![1, 2, 1]; // Keys: [1, 2, 1] -> ["a", null, "a"]
        let nulls = vec![0, 1, 0]; // Null bitmap: [non-null, null, non-null]
        let mut reader = MockReader::new(data);

        let type_ = Type::Enum8(pairs);
        let data_type = arrow::datatypes::DataType::Dictionary(
            Box::new(arrow::datatypes::DataType::Int8),
            Box::new(arrow::datatypes::DataType::Utf8),
        );
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = deserialize_async(&type_, &mut builder, &mut reader, 3, &nulls).await.unwrap();
        let values = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let expected = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(0)]),
                values,
            )
            .unwrap(),
        ) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    #[tokio::test]
    async fn test_deserialize_enum8_empty() {
        let pairs = vec![("a".to_string(), 1_i8), ("b".to_string(), 2_i8)];
        let data = vec![]; // Empty
        let mut reader = MockReader::new(data);

        let type_ = Type::Enum8(pairs);
        let data_type = arrow::datatypes::DataType::Dictionary(
            Box::new(arrow::datatypes::DataType::Int8),
            Box::new(arrow::datatypes::DataType::Utf8),
        );
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = deserialize_async(&type_, &mut builder, &mut reader, 0, &[]).await.unwrap();
        let values = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let expected = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(Vec::<i8>::new()), values)
                .unwrap(),
        ) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    #[tokio::test]
    async fn test_deserialize_enum8_invalid_index() {
        let pairs = vec![("a".to_string(), 1_i8), ("b".to_string(), 2_i8)];
        let data = vec![3]; // Invalid key: 3
        let mut reader = MockReader::new(data);

        let type_ = Type::Enum8(pairs);
        let data_type = arrow::datatypes::DataType::Dictionary(
            Box::new(arrow::datatypes::DataType::Int8),
            Box::new(arrow::datatypes::DataType::Utf8),
        );
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_async(&type_, &mut builder, &mut reader, 1, &[]).await;
        assert!(matches!(
            result,
            Err(Error::ArrowDeserialize(msg))
            if msg.contains("Invalid")
        ));
    }

    #[tokio::test]
    async fn test_deserialize_invalid_type() {
        let data = vec![];
        let mut reader = MockReader::new(data);

        let type_ = Type::Int32;
        let data_type = arrow::datatypes::DataType::Int32;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_async(&type_, &mut builder, &mut reader, 0, &[]).await;
        assert!(matches!(
            result,
            Err(Error::ArrowDeserialize(msg))
            if msg.contains("Unexpected builder")
        ));
    }
}
