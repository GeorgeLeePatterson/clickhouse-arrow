use arrow::array::{Array, ArrayRef, MapArray};
use arrow::datatypes::DataType;
use tokio::io::AsyncWriteExt;

use super::ClickHouseArrowSerializer;
use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

/// Serializes an Arrow `MapArray` to `ClickHouse`’s native format for `Map` types.
///
/// This function writes the `Map` data by serializing the key and value arrays separately, preceded
/// by offsets indicating the total length of key-value pairs for each map entry. It follows the
/// native format’s approach, where a `Map` is represented as a collection of key-value pairs, with
/// offsets tracking the cumulative number of pairs.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` type of the map.
/// - `field`: The Arrow `Field` describing the map’s metadata.
/// - `column`: The `MapArray` containing the map data.
/// - `writer`: The async writer to serialize to (e.g., a TCP stream).
/// - `state`: A mutable `SerializerState` for serialization context.
///
/// # Returns
/// A `Result` indicating success or a `Error` if serialization fails.
///
/// # Errors
/// - Returns `ArrowSerialize` if the `column` is not a `MapArray`, the data type is not `Map`, or
///   the inner struct has incorrect fields.
/// - Returns an error if the `type_hint` is not a `Map` type.
/// - Returns `Io` if writing to the writer fails.
pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    // Unwrap the type hint to get the key and value types
    let (key_type, value_type) = type_hint.unwrap_map()?;

    let map_array = column
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| Error::ArrowSerialize("Expected MapArray for Map type".into()))?;

    // Validate the data type is Map
    let DataType::Map(struct_field, _ordered) = data_type else {
        return Err(Error::ArrowSerialize("Expected Map data type for MapArray".into()));
    };

    // Validate the inner struct has exactly two fields (key, value)
    let DataType::Struct(fields) = struct_field.data_type() else {
        return Err(Error::ArrowSerialize("MapArray field must be a Struct".into()));
    };

    if fields.len() != 2 {
        return Err(Error::ArrowSerialize(
            "MapArray struct must have exactly two fields (key, value)".into(),
        ));
    }

    // Write offsets (total length of key-value pairs per map entry)
    let offsets = map_array.offsets();
    let mut total_length = 0;
    for i in 0..map_array.len() {
        #[expect(clippy::cast_sign_loss)]
        let length = (offsets[i + 1] - offsets[i]) as u64;
        total_length += length;
        writer.write_u64_le(total_length).await?;
    }

    // Serialize keys and values
    let keys = map_array.keys();
    let values = map_array.values();
    key_type.serialize_async(writer, keys, fields[0].data_type(), state).await?;
    value_type.serialize_async(writer, values, fields[1].data_type(), state).await?;

    Ok(())
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    // Unwrap the type hint to get the key and value types
    let (key_type, value_type) = type_hint.unwrap_map()?;

    let map_array = column
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| Error::ArrowSerialize("Expected MapArray for Map type".into()))?;

    // Validate the data type is Map
    let DataType::Map(struct_field, _ordered) = data_type else {
        return Err(Error::ArrowSerialize("Expected Map data type for MapArray".into()));
    };

    // Validate the inner struct has exactly two fields (key, value)
    let DataType::Struct(fields) = struct_field.data_type() else {
        return Err(Error::ArrowSerialize("MapArray field must be a Struct".into()));
    };

    if fields.len() != 2 {
        return Err(Error::ArrowSerialize(
            "MapArray struct must have exactly two fields (key, value)".into(),
        ));
    }

    // Write offsets (total length of key-value pairs per map entry)
    let offsets = map_array.offsets();
    let mut total_length = 0;
    for i in 0..map_array.len() {
        #[expect(clippy::cast_sign_loss)]
        let length = (offsets[i + 1] - offsets[i]) as u64;
        total_length += length;
        writer.put_u64_le(total_length);
    }

    // Serialize keys and values
    let keys = map_array.keys();
    let values = map_array.values();
    key_type.serialize(writer, keys, fields[0].data_type(), state)?;
    value_type.serialize(writer, values, fields[1].data_type(), state)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    #![expect(clippy::clone_on_ref_ptr)]
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array, MapArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use crate::arrow::types::{MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME};
    use crate::formats::SerializerState;
    use crate::native::types::Type;

    type MockWriter = Vec<u8>;

    fn wrap_map_type(key_type: Type, value_type: Type) -> Type {
        Type::Map(Box::new(key_type), Box::new(value_type))
    }

    #[tokio::test]
    async fn test_serialize_map_int32_string() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, false));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into()); // [{1:"a", 2:"b"}, {}, {3:"c"}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(
            &wrap_map_type(Type::Int32, Type::String),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .await
        .unwrap();
        let expected = vec![
            // Offsets: [2, 2, 3]
            2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            // Keys: [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // Values: ["a", "b", "c"]
            1, 97, 1, 98, 1, 99,
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_map_empty() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, false));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let values = Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 0].into()); // [{}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(
            &wrap_map_type(Type::Int32, Type::String),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .await
        .unwrap();
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0]; // Offsets: [0]
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_map_nullable_values() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, true));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into()); // [{1:"a", 2:null}, {}, {3:"c"}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(
            &wrap_map_type(Type::Int32, Type::Nullable(Box::new(Type::String))),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .await
        .unwrap();
        let expected = vec![
            // Offsets: [2, 2, 3]
            2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            // Keys: [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // Values: ["a", null, "c"]
            0, 1, 0, 1, 97, 0, 1, 99,
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_nested_map() {
        // Inner Map(String, Int32)
        let inner_key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false));
        let inner_value_field =
            Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false));
        let inner_fields = Fields::from(vec![inner_key_field, inner_value_field]);

        let inner_keys = Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef;
        let inner_values = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let inner_columns = vec![inner_keys, inner_values];

        let inner_entries = StructArray::new(inner_fields.clone(), inner_columns, None);
        let inner_field =
            Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(inner_fields.clone()), false));

        let inner_offsets = OffsetBuffer::new(vec![0, 1, 2].into()); // [{"x":10}, {"y":20}]

        let inner_map_array = Arc::new(
            MapArray::try_new(inner_field.clone(), inner_offsets, inner_entries, None, false)
                .unwrap(),
        ) as ArrayRef;

        // Outer Map(Int32, Map(String, Int32))
        let outer_key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let outer_value_field = Arc::new(Field::new(
            STRUCT_VALUE_FIELD_NAME,
            inner_map_array.data_type().clone(),
            false,
        ));
        let outer_fields = Fields::from(vec![outer_key_field, outer_value_field]);

        let outer_keys = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let outer_values = inner_map_array;
        let outer_columns = vec![outer_keys, outer_values];

        let outer_entries = StructArray::new(outer_fields.clone(), outer_columns, None);
        let outer_field =
            Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(outer_fields.clone()), false));

        let outer_offsets = OffsetBuffer::new(vec![0, 1, 2].into()); // [{1:{"x":10}}, {2:{"y":20}}]

        let map_array = Arc::new(
            MapArray::try_new(outer_field.clone(), outer_offsets, outer_entries, None, false)
                .unwrap(),
        ) as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(
            &wrap_map_type(Type::Int32, Type::Map(Box::new(Type::String), Box::new(Type::Int32))),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .await
        .unwrap();
        let expected = vec![
            // Offsets: [1, 2]
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, // Keys: [1, 2]
            1, 0, 0, 0, 2, 0, 0, 0,
            // Values: [{"x":10}, {"y":20}]
            // Inner offsets: [1, 2]
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, // Inner keys: ["x", "y"]
            1, 120, 1, 121, // Inner values: [10, 20]
            10, 0, 0, 0, 20, 0, 0, 0,
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_map_single_entry() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Float64, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Float64Array::from(vec![1.5])) as ArrayRef;
        let values = Arc::new(Int32Array::from(vec![100])) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 1].into()); // [{1.5:100}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(
            &wrap_map_type(Type::Float64, Type::Int32),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .await
        .unwrap();
        let expected = vec![
            // Offsets: [1]
            1, 0, 0, 0, 0, 0, 0, 0, // Keys: [1.5]
            0, 0, 0, 0, 0, 0, 248, 63, // Values: [100]
            100, 0, 0, 0,
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_invalid_array_type() {
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        let result = serialize_async(
            &wrap_map_type(Type::Int32, Type::String),
            &mut writer,
            &column,
            &DataType::Int32,
            &mut state,
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("Expected MapArray for Map type")
        ));
    }

    #[tokio::test]
    async fn test_serialize_invalid_data_type() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, false));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec!["a"])) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 1].into()); // [{1:"a"}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        let result = serialize_async(
            &wrap_map_type(Type::Int32, Type::String),
            &mut writer,
            &map_array,
            &DataType::Int32,
            &mut state,
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("Expected Map data type for MapArray")
        ));
    }
}

#[cfg(test)]
mod tests_sync {
    #![expect(clippy::clone_on_ref_ptr)]
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array, MapArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use crate::arrow::types::{MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME};
    use crate::formats::SerializerState;
    use crate::native::types::Type;

    type MockWriter = Vec<u8>;

    fn wrap_map_type(key_type: Type, value_type: Type) -> Type {
        Type::Map(Box::new(key_type), Box::new(value_type))
    }

    #[test]
    fn test_serialize_map_int32_string() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, false));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into()); // [{1:"a", 2:"b"}, {}, {3:"c"}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(
            &wrap_map_type(Type::Int32, Type::String),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .unwrap();
        let expected = vec![
            // Offsets: [2, 2, 3]
            2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            // Keys: [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // Values: ["a", "b", "c"]
            1, 97, 1, 98, 1, 99,
        ];
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_map_empty() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, false));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let values = Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 0].into()); // [{}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(
            &wrap_map_type(Type::Int32, Type::String),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .unwrap();
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0]; // Offsets: [0]
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_map_nullable_values() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, true));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into()); // [{1:"a", 2:null}, {}, {3:"c"}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(
            &wrap_map_type(Type::Int32, Type::Nullable(Box::new(Type::String))),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .unwrap();
        let expected = vec![
            // Offsets: [2, 2, 3]
            2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            // Keys: [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // Values: ["a", null, "c"]
            0, 1, 0, 1, 97, 0, 1, 99,
        ];
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_nested_map() {
        // Inner Map(String, Int32)
        let inner_key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false));
        let inner_value_field =
            Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false));
        let inner_fields = Fields::from(vec![inner_key_field, inner_value_field]);

        let inner_keys = Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef;
        let inner_values = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let inner_columns = vec![inner_keys, inner_values];

        let inner_entries = StructArray::new(inner_fields.clone(), inner_columns, None);
        let inner_field =
            Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(inner_fields.clone()), false));

        let inner_offsets = OffsetBuffer::new(vec![0, 1, 2].into()); // [{"x":10}, {"y":20}]

        let inner_map_array = Arc::new(
            MapArray::try_new(inner_field.clone(), inner_offsets, inner_entries, None, false)
                .unwrap(),
        ) as ArrayRef;

        // Outer Map(Int32, Map(String, Int32))
        let outer_key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let outer_value_field = Arc::new(Field::new(
            STRUCT_VALUE_FIELD_NAME,
            inner_map_array.data_type().clone(),
            false,
        ));
        let outer_fields = Fields::from(vec![outer_key_field, outer_value_field]);

        let outer_keys = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let outer_values = inner_map_array;
        let outer_columns = vec![outer_keys, outer_values];

        let outer_entries = StructArray::new(outer_fields.clone(), outer_columns, None);
        let outer_field =
            Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(outer_fields.clone()), false));

        let outer_offsets = OffsetBuffer::new(vec![0, 1, 2].into()); // [{1:{"x":10}}, {2:{"y":20}}]

        let map_array = Arc::new(
            MapArray::try_new(outer_field.clone(), outer_offsets, outer_entries, None, false)
                .unwrap(),
        ) as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(
            &wrap_map_type(Type::Int32, Type::Map(Box::new(Type::String), Box::new(Type::Int32))),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .unwrap();
        let expected = vec![
            // Offsets: [1, 2]
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, // Keys: [1, 2]
            1, 0, 0, 0, 2, 0, 0, 0,
            // Values: [{"x":10}, {"y":20}]
            // Inner offsets: [1, 2]
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, // Inner keys: ["x", "y"]
            1, 120, 1, 121, // Inner values: [10, 20]
            10, 0, 0, 0, 20, 0, 0, 0,
        ];
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_map_single_entry() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Float64, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Float64Array::from(vec![1.5])) as ArrayRef;
        let values = Arc::new(Int32Array::from(vec![100])) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 1].into()); // [{1.5:100}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(
            &wrap_map_type(Type::Float64, Type::Int32),
            &mut writer,
            &map_array,
            map_array.data_type(),
            &mut state,
        )
        .unwrap();
        let expected = vec![
            // Offsets: [1]
            1, 0, 0, 0, 0, 0, 0, 0, // Keys: [1.5]
            0, 0, 0, 0, 0, 0, 248, 63, // Values: [100]
            100, 0, 0, 0,
        ];
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_invalid_array_type() {
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        let result = serialize(
            &wrap_map_type(Type::Int32, Type::String),
            &mut writer,
            &column,
            &DataType::Int32,
            &mut state,
        );
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("Expected MapArray for Map type")
        ));
    }

    #[test]
    fn test_serialize_invalid_data_type() {
        let key_field = Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false));
        let value_field = Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, false));
        let fields = Fields::from(vec![key_field, value_field]);

        let keys = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec!["a"])) as ArrayRef;
        let columns = vec![keys, values];

        let entries = StructArray::new(fields.clone(), columns, None);
        let field = Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false));

        let offsets = OffsetBuffer::new(vec![0, 1].into()); // [{1:"a"}]

        let map_array =
            Arc::new(MapArray::try_new(field.clone(), offsets, entries, None, false).unwrap())
                as ArrayRef;

        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        let result = serialize(
            &wrap_map_type(Type::Int32, Type::String),
            &mut writer,
            &map_array,
            &DataType::Int32,
            &mut state,
        );
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("Expected Map data type for MapArray")
        ));
    }
}
