/// Deserialization logic for `ClickHouse` `Map` types into Arrow `MapArray`.
///
/// This module provides a function to deserialize `ClickHouse`’s native format for `Map` types
/// into an Arrow `MapArray`, which is a list of key-value pairs stored as a `StructArray` with
/// offsets. It is used by the `ClickHouseArrowDeserializer` implementation in `deserialize.rs`
/// to handle map data.
///
/// The `deserialize` function reads offsets, keys, and values from the reader, constructing a
/// `MapArray` with the specified key and value types.
use std::sync::Arc;

use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use tokio::io::AsyncReadExt;

use super::ClickHouseArrowDeserializer;
use crate::arrow::builder::TypedBuilder;
use crate::arrow::types::MAP_FIELD_NAME;
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::{Error, Result, Type};

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
#[expect(clippy::cast_possible_truncation)]
pub(super) async fn deserialize_async<R: ClickHouseRead>(
    types: (&Type, &Type),
    builder: &mut TypedBuilder,
    data_type: &DataType,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    // First get inner map fields
    let (key_type, value_type) = types;
    let (key_field, value_field) = crate::arrow::builder::map::get_map_fields(data_type)?;

    let TypedBuilder::Map((key_builder, value_builder)) = builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected builder for map: {}",
            builder.as_ref()
        )));
    };

    let offset_bytes = super::list::bulk_offsets!(tokio; reader, rbuffer, rows);
    let offsets: &[u64] = bytemuck::cast_slice::<u8, u64>(&rbuffer[..offset_bytes]);
    let offset_buffer =
        OffsetBuffer::new(offsets.iter().map(|&o| o as i32).collect::<ScalarBuffer<_>>());
    let total_pairs = *offsets.last().unwrap_or(&0) as usize;

    let key_array = key_type
        .deserialize_arrow_async(
            key_builder,
            reader,
            key_field.data_type(),
            total_pairs,
            &[],
            rbuffer,
        )
        .await?;
    let value_array = value_type
        .deserialize_arrow_async(
            value_builder,
            reader,
            value_field.data_type(),
            total_pairs,
            &[],
            rbuffer,
        )
        .await?;

    // Construct StructArray for entries
    let struct_field = Arc::new(Field::new(
        MAP_FIELD_NAME,
        DataType::Struct(Fields::from(vec![Arc::clone(key_field), Arc::clone(value_field)])),
        false,
    ));
    let struct_fields =
        vec![(Arc::clone(key_field), key_array), (Arc::clone(value_field), value_array)];

    // Construct MapArray
    let null_buffer = if nulls.is_empty() {
        None
    } else {
        Some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()))
    };

    let struct_arr = StructArray::from(struct_fields);
    Ok(Arc::new(MapArray::new(struct_field, offset_buffer, struct_arr, null_buffer, false)))
}

#[allow(dead_code)] // TODO: remove once synchronous Arrow path is fully retired
pub(super) fn deserialize<R: ClickHouseBytesRead>(
    builders: (&mut TypedBuilder, &mut TypedBuilder),
    reader: &mut R,
    key_value: (&Type, &Type),
    data_type: &DataType,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    let (key_b, value_b) = builders;
    let (kt, vt) = key_value;

    let (key_field, value_field) = crate::arrow::builder::map::get_map_fields(data_type)?;
    let key_dt = key_field.data_type();
    let value_dt = value_field.data_type();

    let offset_bytes = super::list::bulk_offsets!(reader, rbuffer, rows);
    let offsets: &[u64] = bytemuck::cast_slice::<u8, u64>(&rbuffer[..offset_bytes]);
    #[expect(clippy::cast_possible_truncation)]
    let (offset_buffer, total_pairs) = {
        (
            OffsetBuffer::new(offsets.iter().map(|&o| o as i32).collect::<ScalarBuffer<_>>()),
            *offsets.last().unwrap_or(&0) as usize,
        )
    };

    // Read keys and values
    let key_array = kt.deserialize_arrow(key_b, reader, key_dt, total_pairs, &[], rbuffer)?;
    let value_array = vt.deserialize_arrow(value_b, reader, value_dt, total_pairs, &[], rbuffer)?;

    // Construct StructArray for entries
    let struct_field = Arc::new(Field::new(
        MAP_FIELD_NAME,
        DataType::Struct(Fields::from(vec![Arc::clone(key_field), Arc::clone(value_field)])),
        false,
    ));
    let struct_fields =
        vec![(Arc::clone(key_field), key_array), (Arc::clone(value_field), value_array)];

    // Construct MapArray
    let null_buffer = if nulls.is_empty() {
        None
    } else {
        Some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()))
    };

    let struct_arr = StructArray::from(struct_fields);
    Ok(Arc::new(MapArray::new(struct_field, offset_buffer, struct_arr, null_buffer, false)))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{
        Array, Int32Array, MapArray, StringArray, StructArray, TimestampSecondArray,
    };
    use chrono_tz::Tz;

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::block::{STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME};
    use crate::arrow::ch_to_arrow_type;
    use crate::native::types::Type;

    fn create_map_type(key: &Type, value: &Type, nullable: bool) -> DataType {
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        let (key_type, nil) = ch_to_arrow_type(key, opts).unwrap();
        let key_field = Field::new(STRUCT_KEY_FIELD_NAME, key_type, nil);
        let (value_type, nil) = ch_to_arrow_type(value, opts).unwrap();
        let value_field = Field::new(STRUCT_VALUE_FIELD_NAME, value_type, nil);
        let inner = DataType::Struct(Fields::from(vec![key_field, value_field]));
        let field = Arc::new(Field::new(MAP_FIELD_NAME, inner, nullable));
        DataType::Map(field, false)
    }

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
        let data_type = create_map_type(&key_type, &value_type, false);
        let mut builder = TypedBuilder::try_new(
            &Type::Map(Box::new(key_type.clone()), Box::new(value_type.clone())),
            &data_type,
        )
        .unwrap();
        let result = deserialize_async(
            (&key_type, &value_type),
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type = create_map_type(&key_type, &value_type, true);
        let mut builder = TypedBuilder::try_new(
            &Type::Map(Box::new(key_type.clone()), Box::new(value_type.clone())),
            &data_type,
        )
        .unwrap();
        let result = deserialize_async(
            (&key_type, &value_type),
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type = create_map_type(&key_type, &value_type, false);
        let mut builder = TypedBuilder::try_new(
            &Type::Map(Box::new(key_type.clone()), Box::new(value_type.clone())),
            &data_type,
        )
        .unwrap();
        let result = deserialize_async(
            (&key_type, &value_type),
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type = create_map_type(&key_type, &value_type, false);
        let mut builder = TypedBuilder::try_new(
            &Type::Map(Box::new(key_type.clone()), Box::new(value_type.clone())),
            &data_type,
        )
        .unwrap();
        let result = deserialize_async(
            (&key_type, &value_type),
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type = create_map_type(&key_type, &value_type, false);
        let mut builder = TypedBuilder::try_new(
            &Type::Map(Box::new(key_type.clone()), Box::new(value_type.clone())),
            &data_type,
        )
        .unwrap();
        let result = deserialize_async(
            (&key_type, &value_type),
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type = create_map_type(&key_type, &value_type, false);
        let mut builder = TypedBuilder::try_new(
            &Type::Map(Box::new(key_type.clone()), Box::new(value_type.clone())),
            &data_type,
        )
        .unwrap();
        let result = deserialize_async(
            (&key_type, &value_type),
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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

#[cfg(test)]
mod tests_sync {
    use std::io::Cursor;

    use arrow::array::{
        Array, Int32Array, MapArray, StringArray, StructArray, TimestampSecondArray,
    };
    use chrono_tz::Tz;

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::block::{STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME};
    use crate::arrow::ch_to_arrow_type;
    use crate::native::types::Type;

    fn create_map_type(
        key: &Type,
        value: &Type,
        nullable: bool,
    ) -> Result<(DataType, TypedBuilder, TypedBuilder)> {
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        let (key_type, nil) = ch_to_arrow_type(key, opts).unwrap();
        let key_builder = TypedBuilder::try_new(key, &key_type)?;
        let key_field = Field::new(STRUCT_KEY_FIELD_NAME, key_type, nil);
        let (value_type, nil) = ch_to_arrow_type(value, opts).unwrap();
        let value_builder = TypedBuilder::try_new(value, &value_type)?;
        let value_field = Field::new(STRUCT_VALUE_FIELD_NAME, value_type, nil);
        let inner = DataType::Struct(Fields::from(vec![key_field, value_field]));
        let field = Arc::new(Field::new(MAP_FIELD_NAME, inner, nullable));
        let dt = DataType::Map(field, false);
        Ok((dt, key_builder, value_builder))
    }

    /// Tests deserialization of `Map(Int32, String)` with non-nullable values.
    #[test]
    fn test_deserialize_map_int32_string() {
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
        let (dt, mut kb, mut vb) = create_map_type(&key_type, &value_type, false).unwrap();
        let result = deserialize(
            (&mut kb, &mut vb),
            &mut reader,
            (&key_type, &value_type),
            &dt,
            rows,
            &nulls,
            &mut vec![],
        )
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
    #[test]
    fn test_deserialize_nullable_map_int32_string() {
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
        let (dt, mut kb, mut vb) = create_map_type(&key_type, &value_type, true).unwrap();
        let result = deserialize(
            (&mut kb, &mut vb),
            &mut reader,
            (&key_type, &value_type),
            &dt,
            rows,
            &nulls,
            &mut vec![],
        )
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
    #[test]
    fn test_deserialize_map_int32_nullable_int32() {
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
        let (dt, mut kb, mut vb) = create_map_type(&key_type, &value_type, false).unwrap();
        let result = deserialize(
            (&mut kb, &mut vb),
            &mut reader,
            (&key_type, &value_type),
            &dt,
            rows,
            &nulls,
            &mut vec![],
        )
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
    #[test]
    fn test_deserialize_map_string_datetime() {
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
        let (dt, mut kb, mut vb) = create_map_type(&key_type, &value_type, false).unwrap();
        let result = deserialize(
            (&mut kb, &mut vb),
            &mut reader,
            (&key_type, &value_type),
            &dt,
            rows,
            &nulls,
            &mut vec![],
        )
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
    #[test]
    fn test_deserialize_map_zero_rows() {
        let key_type = Type::Int32;
        let value_type = Type::String;
        let rows = 0;
        let nulls = vec![];
        let input = vec![]; // No data for zero rows
        let mut reader = Cursor::new(input);
        let (dt, mut kb, mut vb) = create_map_type(&key_type, &value_type, false).unwrap();
        let result = deserialize(
            (&mut kb, &mut vb),
            &mut reader,
            (&key_type, &value_type),
            &dt,
            rows,
            &nulls,
            &mut vec![],
        )
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
    #[test]
    fn test_deserialize_map_empty_inner() {
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
        let (dt, mut kb, mut vb) = create_map_type(&key_type, &value_type, false).unwrap();
        let result = deserialize(
            (&mut kb, &mut vb),
            &mut reader,
            (&key_type, &value_type),
            &dt,
            rows,
            &nulls,
            &mut vec![],
        )
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
