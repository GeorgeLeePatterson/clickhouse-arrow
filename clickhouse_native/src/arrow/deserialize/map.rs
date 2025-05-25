/// Deserialization logic for `ClickHouse` `Map` types into Arrow `MapArray`.
///
/// This module provides a function to deserialize ClickHouse’s native format for `Map` types
/// into an Arrow `MapArray`, which is a list of key-value pairs stored as a `StructArray` with
/// offsets. It is used by the `ClickhouseArrowDeserializer` implementation in `deserialize.rs`
/// to handle map data.
///
/// The `deserialize` function reads offsets, keys, and values from the reader, constructing a
/// `MapArray` with the specified key and value types.
use std::sync::Arc;

use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use tokio::io::AsyncReadExt;

use super::ClickhouseArrowDeserializer;
use crate::arrow::types::{MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME};
use crate::formats::DeserializerState;
use crate::io::ClickhouseRead;
use crate::{Result, Type};

/// Deserializes a `ClickHouse` `Map` type into an Arrow `MapArray`.
///
/// Reads the offsets (cumulative lengths of key-value pairs), followed by the key and value arrays.
/// Constructs a `MapArray` with a `StructArray` containing the keys and values, respecting
/// nullability.
///
/// # Arguments
/// - `key_type`: The `ClickHouse` type of the map keys.
/// - `value_type`: The `ClickHouse` type of the map values.
/// - `reader`: The async reader providing the `ClickHouse` native format data.
/// - `rows`: The number of map entries to deserialize.
/// - `nulls`: A slice indicating null values (`1` for null, `0` for non-null).
/// - `state`: A mutable `DeserializerState` for deserialization context.
///
/// # Returns
/// A `Result` containing the deserialized `MapArray` as an `ArrayRef` or a `Error`
/// if deserialization fails.
///
/// # Errors
/// - Returns `ArrowDeserialize` if the key or value type is unsupported or the data is malformed.
/// - Returns `Io` if reading from the reader fails.
pub(super) async fn deserialize<R: ClickhouseRead>(
    key_type: &Type,
    value_type: &Type,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    state: &mut DeserializerState,
) -> Result<ArrayRef> {
    // Read offsets
    let mut offsets = vec![0_i32];
    for _ in 0..rows {
        #[allow(clippy::cast_possible_truncation)]
        let offset = reader.read_u64_le().await? as i32;
        offsets.push(offset);
    }

    // Total number of key-value pairs
    #[allow(clippy::cast_sign_loss)]
    let total_pairs = offsets[rows] as usize;

    // Read keys and values
    let key_array = key_type.deserialize(reader, total_pairs, &[], state).await?;
    let value_array = value_type.deserialize(reader, total_pairs, &[], state).await?;

    // Construct StructArray for entries
    let key_field = Arc::new(Field::new(
        STRUCT_KEY_FIELD_NAME,
        key_array.data_type().clone(),
        key_type.is_nullable(),
    ));
    let value_field = Arc::new(Field::new(
        STRUCT_VALUE_FIELD_NAME,
        value_array.data_type().clone(),
        value_type.strip_low_cardinality().is_nullable(),
    ));
    let struct_field = Arc::new(Field::new(
        MAP_FIELD_NAME,
        DataType::Struct(Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)])),
        false,
    ));
    let struct_array = StructArray::from(vec![(key_field, key_array), (value_field, value_array)]);

    // Construct MapArray
    let null_buffer = if nulls.is_empty() {
        None
    } else {
        Some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()))
    };

    Ok(Arc::new(MapArray::new(
        struct_field,
        OffsetBuffer::new(offsets.into()),
        struct_array,
        null_buffer,
        false,
    )) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{
        Array, Int32Array, MapArray, StringArray, StructArray, TimestampSecondArray,
    };
    use chrono_tz::Tz;

    use super::*;
    use crate::native::types::Type;

    /// Tests deserialization of `Map(Int32, String)` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_map_int32_string() {
        let key_type = Type::Int32;
        let value_type = Type::String;
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Keys (Int32): [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
            // Values (String): ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&key_type, &value_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Map(Int32, String)");
        let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let values = struct_array.column(1).as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(map_array.len(), 3);
        assert_eq!(keys, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(values, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(map_array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Map(Int32, String))` with null maps.
    #[tokio::test]
    async fn test_deserialize_nullable_map_int32_string() {
        let key_type = Type::Int32;
        let value_type = Type::String;
        let rows = 3;
        let nulls = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Keys (Int32): [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
            // Values (String): ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&key_type, &value_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Map(Int32, String))");
        let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let values = struct_array.column(1).as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(map_array.len(), 3);
        assert_eq!(keys, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(values, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(
            map_array.nulls().unwrap().iter().collect::<Vec<bool>>(),
            vec![true, false, true] // 0=not null, 1=null
        );
    }

    /// Tests deserialization of `Map(Int32, Nullable(Int32))` with nullable values.
    #[tokio::test]
    async fn test_deserialize_map_int32_nullable_int32() {
        let key_type = Type::Int32;
        let value_type = Type::Nullable(Box::new(Type::Int32));
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Keys (Int32): [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
            // Values (Nullable(Int32)): [10, null, 30, null, 50]
            // Null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0, // Values: [10, 0, 30, 0, 50]
            10, 0, 0, 0, // 10
            0, 0, 0, 0, // null
            30, 0, 0, 0, // 30
            0, 0, 0, 0, // null
            50, 0, 0, 0, // 50
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&key_type, &value_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Map(Int32, Nullable(Int32))");
        let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let values = struct_array.column(1).as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(map_array.len(), 3);
        assert_eq!(keys, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(values, &Int32Array::from(vec![Some(10), None, Some(30), None, Some(50)]));
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(map_array.nulls(), None);
    }

    /// Tests deserialization of `Map(String, DateTime)` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_map_string_datetime() {
        let key_type = Type::String;
        let value_type = Type::DateTime(Tz::UTC);
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 4] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            4, 0, 0, 0, 0, 0, 0, 0, // 4
            // Keys (String): ["a", "b", "c", "d"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            // Values (DateTime): [1000, 2000, 3000, 4000]
            232, 3, 0, 0, // 1000
            208, 7, 0, 0, // 2000
            184, 11, 0, 0, // 3000
            160, 15, 0, 0, // 4000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&key_type, &value_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Map(String, DateTime)");
        let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let values =
            struct_array.column(1).as_any().downcast_ref::<TimestampSecondArray>().unwrap();

        let tz =
            TimestampSecondArray::from(vec![1000, 2000, 3000, 4000]).with_timezone_opt(Some("UTC"));
        assert_eq!(map_array.len(), 2);
        assert_eq!(keys, &StringArray::from(vec!["a", "b", "c", "d"]));
        assert_eq!(values, &tz);
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 4]);
        assert_eq!(map_array.nulls(), None);
    }

    /// Tests deserialization of `Map(Int32, String)` with zero rows.
    #[tokio::test]
    async fn test_deserialize_map_zero_rows() {
        let key_type = Type::Int32;
        let value_type = Type::String;
        let rows = 0;
        let nulls = vec![];
        let input = vec![]; // No data for zero rows
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&key_type, &value_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Map(Int32, String) with zero rows");
        let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let values = struct_array.column(1).as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(map_array.len(), 0);
        assert_eq!(keys, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(values, &StringArray::from(Vec::<String>::new()));
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0]);
        assert_eq!(map_array.nulls(), None);
    }

    /// Tests deserialization of `Map(Int32, String)` with empty maps.
    #[tokio::test]
    async fn test_deserialize_map_empty_inner() {
        let key_type = Type::Int32;
        let value_type = Type::String;
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Offsets: [0, 0] (skipping first 0)
            0, 0, 0, 0, 0, 0, 0, 0, // 0
            0, 0, 0, 0, 0, 0, 0, 0, /* 0
                * No keys or values */
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&key_type, &value_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Map(Int32, String) with empty inner maps");
        let map_array = result.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let values = struct_array.column(1).as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(map_array.len(), 2);
        assert_eq!(keys, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(values, &StringArray::from(Vec::<String>::new()));
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 0, 0]);
        assert_eq!(map_array.nulls(), None);
    }
}
