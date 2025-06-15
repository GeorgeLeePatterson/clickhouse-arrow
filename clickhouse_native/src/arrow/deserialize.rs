/// Deserialization logic for converting ClickHouse’s native format into Arrow arrays.
///
/// This module defines the `ClickhouseArrowDeserializer` trait and implements it for `Type`,
/// enabling the conversion of ClickHouse’s native format data into Arrow arrays (e.g.,
/// `Int32Array`, `StringArray`, `ListArray`). It is used by the [`super::ProtocolData`]
/// implementation in `arrow.rs` to deserialize data received from `ClickHouse` into
/// `RecordBatch`es.
///
/// The `deserialize` function dispatches to specialized modules based on the `Type` variant,
/// handling primitive types, strings, nullable types, arrays, low cardinality dictionaries, enums,
/// maps, and tuples. Each module processes the input data from a `ClickhouseRead` reader,
/// respecting nullability and maintaining deserialization state.
mod binary;
mod enums;
mod list;
mod low_cardinality;
mod map;
mod null;
mod primitive;
mod tuple;

use arrow::array::*;
use arrow::datatypes::*;
use async_trait::async_trait;

use super::types::ch_to_arrow_type;
use crate::formats::DeserializerState;
use crate::geo::normalize_geo_type;
use crate::io::ClickhouseRead;
use crate::{ArrowOptions, Result, Type};

/// Trait for deserializing ClickHouse’s native format into Arrow arrays.
///
/// Implementations convert data from a `ClickhouseRead` reader into an `ArrayRef`, handling
/// nullability and maintaining deserialization state. The trait is used to map ClickHouse types to
/// Arrow data types and deserialize rows of data.
///
/// # Methods
/// - `arrow_type`: Maps the ClickHouse type to an Arrow `DataType` and nullability flag.
/// - `deserialize`: Reads data from the reader and constructs an `ArrayRef` for the specified
///   number of rows, using nulls and state.
#[async_trait]
pub(crate) trait ClickhouseArrowDeserializer {
    /// Maps the ClickHouse type to an Arrow `DataType` and nullability flag.
    ///
    /// # Arguments
    /// - `strings_as_strings`: If `true`, ClickHouse `String` types are mapped to Arrow `Utf8`;
    ///   otherwise, to `Binary`.
    ///
    /// # Returns
    /// A `Result` containing the `(DataType, is_nullable)` tuple or a `Error` if
    /// the type is unsupported.
    fn arrow_type(&self, options: Option<ArrowOptions>) -> Result<(DataType, bool)>;

    /// Deserializes data from a ClickHouse reader into an Arrow array.
    ///
    /// # Arguments
    /// - `reader`: The async reader providing the ClickHouse native format data.
    /// - `rows`: The number of rows to deserialize.
    /// - `nulls`: A slice indicating null values (`1` for null, `0` for non-null).
    /// - `state`: A mutable `DeserializerState` for maintaining deserialization context.
    ///
    /// # Returns
    /// A `Result` containing the deserialized `ArrayRef` or a `Error` if
    /// deserialization fails.
    async fn deserialize<R: ClickhouseRead>(
        &self,
        reader: &mut R,
        rows: usize,
        nulls: &[u8],
        state: &mut DeserializerState,
    ) -> Result<ArrayRef>;
}

/// Deserializes a ClickHouse `Type` into an Arrow array.
///
/// This implementation dispatches to specialized modules based on the `Type` variant:
/// - Primitives: `Int8`, `Float64`, `Date`, `IPv4`, `String`, `Uuid`, etc. via
///   `primitive::deserialize`.
/// - Nullable types: `Nullable(inner)` via `null::deserialize`.
/// - Arrays: `Array(inner)` via `array::deserialize`.
/// - Low cardinality: `LowCardinality(inner)` via `low_cardinality::deserialize`.
/// - Enums: `Enum8`, `Enum16` via `enums::deserialize`.
/// - Maps: `Map(key, value)` via `map::deserialize`.
/// - Tuples: `Tuple(inner)` via `tuple::deserialize`.
///
/// # Examples
/// ```rust,ignore
/// use arrow::array::{ArrayRef, StringArray};
/// use clickhouse_native::native::types::{Type, DeserializerState};
/// use std::sync::Arc;
/// use tokio::io::Cursor;
///
/// let data = vec![
///     5,                              // Size
///     b'h', b'e', b'l', b'l', b'o',   // "hello"
///     3,                              // Size
///     b'f', b'o', b'o',               // "foo"
/// ];
/// let mut reader = Cursor::new(data);
///         let mut state = DeserializerState::default();
/// let array = Type::String
///     .deserialize(&mut reader, 2, &[], &mut state)
///     .await
///     .unwrap();
/// let expected = Arc::new(StringArray::from(vec!["hello", "foo"])) as ArrayRef;
/// assert_eq!(array.as_ref(), expected.as_ref());
/// ```
#[async_trait]
impl ClickhouseArrowDeserializer for Type {
    fn arrow_type(&self, options: Option<ArrowOptions>) -> Result<(DataType, bool)> {
        ch_to_arrow_type(self, options)
    }

    // TODO: Normalize all deserialize functions to take `self`
    //
    // TODO: Consider encoding a new null mask condition, nulls[i] > 1 == Skip entirely. This would
    // be useful in the case where Dictionaries will contain a None value for nullable values since
    // clickhouse prepends dictionary values with a value representing null (the default value).
    async fn deserialize<R: ClickhouseRead>(
        &self,
        reader: &mut R,
        rows: usize,
        nulls: &[u8],
        state: &mut DeserializerState,
    ) -> Result<ArrayRef> {
        Ok(match self {
            // Primitive types
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::Float32
            | Type::Float64
            | Type::Date
            | Type::Date32
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_) => {
                primitive::deserialize(self, reader, rows, nulls, state).await?
            }
            // String/Binary
            Type::String
            | Type::FixedSizedString(_)
            | Type::Binary
            | Type::FixedSizedBinary(_)
            | Type::Object
            // Special Binary types
            | Type::Int128
            | Type::Int256
            | Type::UInt128
            | Type::UInt256
            | Type::Ipv6
            | Type::Uuid
            | Type::Ipv4 => binary::deserialize(self, reader, rows, nulls, state).await?,
            // Nullable
            Type::Nullable(inner) => null::deserialize(inner, reader, rows, state).await?,
            // Array
            Type::Array(inner) => list::deserialize(inner, reader, rows, nulls, state).await?,
            // LowCardinality
            Type::LowCardinality(inner) => {
                low_cardinality::deserialize(inner, reader, rows, nulls, state).await?
            }
            // Enum
            Type::Enum8(_) | Type::Enum16(_) => {
                enums::deserialize(self, reader, rows, nulls, state).await?
            }
            // Map
            Type::Map(key, value) => {
                map::deserialize(key, value, reader, rows, nulls, state).await?
            }
            // Tuple
            Type::Tuple(inner) => tuple::deserialize(inner, reader, rows, nulls, state).await?,
            // Geo types
            Type::Polygon | Type::MultiPolygon | Type::Point | Type::Ring => {
                // Geo types should be converted earlier, this is a fallback
                let normalized = normalize_geo_type(self).unwrap();
                normalized.deserialize(reader, rows, nulls, state).await?
            }
        })
    }
}

// TODO: Remove - geo unit tests
#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::DataType;

    use super::*;
    use crate::formats::DeserializerState;
    use crate::native::types::Type;

    /// Tests `arrow_type` for `Int32` (non-nullable).
    #[test]
    fn test_arrow_type_int32() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let (data_type, is_nullable) = Type::Int32.arrow_type(options).unwrap();
        assert_eq!(data_type, DataType::Int32);
        assert!(!is_nullable);
    }

    /// Tests `arrow_type` for `Nullable(Int32)`.
    #[test]
    fn test_arrow_type_nullable_int32() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let (data_type, is_nullable) =
            Type::Nullable(Box::new(Type::Int32)).arrow_type(options).unwrap();
        assert_eq!(data_type, DataType::Int32);
        assert!(is_nullable);
    }

    /// Tests `arrow_type` for `String` with `strings_as_strings=true`.
    #[test]
    fn test_arrow_type_string_utf8() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let (data_type, is_nullable) = Type::String.arrow_type(options).unwrap();
        assert_eq!(data_type, DataType::Utf8);
        assert!(!is_nullable);
    }

    /// Tests `arrow_type` for `String` with `strings_as_strings=false`.
    #[test]
    fn test_arrow_type_string_binary() {
        let (data_type, is_nullable) = Type::String.arrow_type(None).unwrap();
        assert_eq!(data_type, DataType::Binary);
        assert!(!is_nullable);
    }

    /// Tests deserialization of `Int32` array.
    #[tokio::test]
    async fn test_deserialize_int32() {
        let input = vec![
            // Values: [1, 2, 3]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let array = Type::Int32.deserialize(&mut reader, 3, &[], &mut state).await.unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Nullable(Int32)` array with nulls.
    #[tokio::test]
    async fn test_deserialize_nullable_int32() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Values: [1, 0, 3]
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let array = Type::Nullable(Box::new(Type::Int32))
            .deserialize(&mut reader, 3, &[], &mut state)
            .await
            .unwrap();
        let expected = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `String` array.
    #[tokio::test]
    async fn test_deserialize_string() {
        let input = vec![
            // Values: ["hello", "", "world"]
            5, b'h', b'e', b'l', b'l', b'o', // "hello"
            0,    // ""
            5, b'w', b'o', b'r', b'l', b'd', // "world"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let array = Type::String.deserialize(&mut reader, 3, &[], &mut state).await.unwrap();
        let expected = Arc::new(StringArray::from(vec!["hello", "", "world"])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Nullable(String)` array with nulls.
    #[tokio::test]
    async fn test_deserialize_nullable_string() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Values: ["a", "", "c"]
            1, b'a', // "a"
            0,    // null (empty string)
            1, b'c', // "c"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let array = Type::Nullable(Box::new(Type::String))
            .deserialize(&mut reader, 3, &[], &mut state)
            .await
            .unwrap();
        let expected = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Array(Int32)` with non-nullable inner values.
    #[tokio::test]
    async fn test_deserialize_array_int32() {
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let array = Type::Array(Box::new(Type::Int32))
            .deserialize(&mut reader, 3, &[], &mut state)
            .await
            .unwrap();
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Array(Int32))` with null arrays.
    #[tokio::test]
    async fn test_deserialize_nullable_array_int32() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Offsets: [2, 2, 5] (skipping first 0, null array repeats offset)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (null)
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let array = Type::Nullable(Box::new(Type::Array(Box::new(Type::Int32))))
            .deserialize(&mut reader, 3, &[], &mut state)
            .await
            .unwrap();
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 2, 5]);
        assert_eq!(list_array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![
            true, false, true
        ]);
    }

    /// Tests deserialization of `Map(String, Int32)` with non-nullable key-value pairs.
    #[tokio::test]
    async fn test_deserialize_map_string_int32() {
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Keys: ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let array = Type::Map(Box::new(Type::String), Box::new(Type::Int32))
            .deserialize(&mut reader, 3, &[], &mut state)
            .await
            .unwrap();
        let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let values = struct_array.column(1).as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(map_array.len(), 3);
        assert_eq!(keys, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(map_array.nulls(), None);
    }

    /// Tests deserialization of `Int32` array with zero rows.
    #[tokio::test]
    async fn test_deserialize_int32_zero_rows() {
        let input = vec![];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let array = Type::Int32.deserialize(&mut reader, 0, &[], &mut state).await.unwrap();
        let expected = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }
}
