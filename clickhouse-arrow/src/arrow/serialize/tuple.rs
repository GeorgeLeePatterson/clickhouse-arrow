use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::DataType;

use super::ClickHouseArrowSerializer;
use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::native::types::Type;
use crate::{Error, Result};

/// Serializes an Arrow `StructArray` to `ClickHouse`’s native format for `Tuple` types.
///
/// This function writes the `Tuple` data by serializing each field of the `StructArray` as a
/// separate column, delegating to the inner types’ serialization logic. It follows the native
/// format’s approach, where a `Tuple` is represented as a collection of columns.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` types of the tuple.
/// - `column`: The `StructArray` containing the tuple data.
/// - `writer`: The async writer to serialize to (e.g., a TCP stream).
/// - `state`: A mutable `SerializerState` for serialization context.
///
/// # Returns
/// A `Result` indicating success or a `Error` if serialization fails.
///
/// # Errors
/// - Returns `ArrowSerialize` if the `column` is not a `StructArray` or the number of fields
///   doesn’t match the tuple’s inner types.
/// - Returns an error if the `type_hint` is not a `Tuple` type.
/// - Returns `Io` if writing to the writer fails.
pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    state: &mut SerializerState,
) -> Result<()> {
    // Unwrap the tuple
    let inner_types = type_hint.strip_null().unwrap_tuple()?;

    let struct_array = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::ArrowSerialize("Expected StructArray for Tuple type".into()))?;

    // Validate field count
    let DataType::Struct(fields) = struct_array.data_type() else {
        return Err(Error::ArrowSerialize("StructArray must have Struct data type".into()));
    };

    if fields.len() != inner_types.len() {
        return Err(Error::ArrowSerialize(format!(
            "StructArray has {} fields, but Tuple expects {}",
            fields.len(),
            inner_types.len()
        )));
    }

    // Serialize each field as a column
    for (i, (inner_type, field)) in inner_types.iter().zip(fields.iter()).enumerate() {
        let column = struct_array.column(i);
        inner_type.serialize_async(writer, column, field.data_type(), state).await?;
    }

    Ok(())
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    state: &mut SerializerState,
) -> Result<()> {
    // Unwrap the tuple
    let inner_types = type_hint.strip_null().unwrap_tuple()?;

    let struct_array = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::ArrowSerialize("Expected StructArray for Tuple type".into()))?;

    // Validate field count
    let DataType::Struct(fields) = struct_array.data_type() else {
        return Err(Error::ArrowSerialize("StructArray must have Struct data type".into()));
    };

    if fields.len() != inner_types.len() {
        return Err(Error::ArrowSerialize(format!(
            "StructArray has {} fields, but Tuple expects {}",
            fields.len(),
            inner_types.len()
        )));
    }

    // Serialize each field as a column
    for (i, (inner_type, field)) in inner_types.iter().zip(fields.iter()).enumerate() {
        let column = struct_array.column(i);
        inner_type.serialize(writer, column, field.data_type(), state)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int8Array, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, FieldRef};

    use super::*;
    use crate::arrow::types::TUPLE_FIELD_NAME_PREFIX;
    use crate::formats::SerializerState;
    use crate::native::types::Type;

    type MockWriter = Vec<u8>;

    fn wrap_tuple(inner: Vec<Type>) -> Type { Type::Tuple(inner) }

    #[tokio::test]
    async fn test_serialize_tuple_int32_string() {
        let data = vec![
            (
                Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}2"), DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
        ];
        let struct_array = Arc::new(StructArray::from(data.clone())) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32, Type::String]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(&type_hint, &mut writer, &struct_array, &mut state).await.unwrap();
        let expected = vec![
            // Int32 column: [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // String column: ["a", "b", "c"]
            1, 97, 1, 98, 1, 99,
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_tuple_empty() {
        let fields = vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false),
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}2"), DataType::Utf8, false),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
        ];
        let struct_array = Arc::new(StructArray::from(
            fields.into_iter().zip(arrays.into_iter()).collect::<Vec<(FieldRef, ArrayRef)>>(),
        )) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32, Type::String]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(&type_hint, &mut writer, &struct_array, &mut state).await.unwrap();
        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn test_serialize_tuple_nullable() {
        let fields = vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, true),
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}2"), DataType::Utf8, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ];
        let struct_array = Arc::new(StructArray::from(
            fields.into_iter().zip(arrays.into_iter()).collect::<Vec<(FieldRef, ArrayRef)>>(),
        )) as ArrayRef;
        let type_hint = wrap_tuple(vec![
            Type::Nullable(Box::new(Type::Int32)),
            Type::Nullable(Box::new(Type::String)),
        ]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(&type_hint, &mut writer, &struct_array, &mut state).await.unwrap();
        let expected = vec![
            0, 1, 0, // Nullable(Int32) column: [1, null, 3]
            1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, // Nullable(String) column: ["a", null, "c"]
            0, 1, 0, 1, 97, 0, 1, 99,
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_nested_tuple() {
        let inner_fields = vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}2"), DataType::Int8, false),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let inner_arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["x", "y"])),
            Arc::new(Int8Array::from(vec![10, 20])),
        ];
        let inner_struct = Arc::new(StructArray::from(
            inner_fields
                .clone()
                .into_iter()
                .zip(inner_arrays.into_iter())
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        ));
        let outer_fields = vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false),
            Field::new(
                format!("{TUPLE_FIELD_NAME_PREFIX}2"),
                DataType::Struct(inner_fields.into()),
                false,
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let outer_arrays: Vec<ArrayRef> =
            vec![Arc::new(Int32Array::from(vec![1, 2])), inner_struct];
        let struct_array = Arc::new(StructArray::from(
            outer_fields
                .into_iter()
                .zip(outer_arrays.into_iter())
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32, Type::Tuple(vec![Type::String, Type::Int8])]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(&type_hint, &mut writer, &struct_array, &mut state).await.unwrap();
        let expected = vec![
            // Int32 column: [1, 2]
            1, 0, 0, 0, 2, 0, 0, 0,
            // Tuple(String, Int8) column: [("x", 10), ("y", 20)]
            // String column: ["x", "y"]
            1, 120, 1, 121, // Int8 column: [10, 20]
            10, 20,
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_single_element_tuple() {
        let fields = vec![(
            Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef,
        )];
        let struct_array = Arc::new(StructArray::from(fields.clone())) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Float64]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize_async(&type_hint, &mut writer, &struct_array, &mut state).await.unwrap();
        let expected = vec![
            // Float64 column: [1.5, 2.5]
            0, 0, 0, 0, 0, 0, 248, 63, // 1.5 (0x3FF8000000000000)
            0, 0, 0, 0, 0, 0, 4, 64, // 2.5 (0x4004000000000000)
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_invalid_array_type() {
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        let result = serialize_async(&type_hint, &mut writer, &column, &mut state).await;
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("Expected StructArray for Tuple type")
        ));
    }

    #[tokio::test]
    async fn test_serialize_mismatched_field_count() {
        let fields = vec![(
            Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )];
        let struct_array = Arc::new(StructArray::from(fields)) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32, Type::String]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        let result = serialize_async(&type_hint, &mut writer, &struct_array, &mut state).await;
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("StructArray has 1 fields, but Tuple expects 2")
        ));
    }
}

#[cfg(test)]
mod tests_sync {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int8Array, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, FieldRef};

    use super::*;
    use crate::arrow::types::TUPLE_FIELD_NAME_PREFIX;
    use crate::formats::SerializerState;
    use crate::native::types::Type;

    type MockWriter = Vec<u8>;

    fn wrap_tuple(inner: Vec<Type>) -> Type { Type::Tuple(inner) }

    #[test]
    fn test_serialize_tuple_int32_string() {
        let data = vec![
            (
                Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}2"), DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
        ];
        let struct_array = Arc::new(StructArray::from(data.clone())) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32, Type::String]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(&type_hint, &mut writer, &struct_array, &mut state).unwrap();
        let expected = vec![
            // Int32 column: [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // String column: ["a", "b", "c"]
            1, 97, 1, 98, 1, 99,
        ];
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_tuple_empty() {
        let fields = vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false),
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}2"), DataType::Utf8, false),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
        ];
        let struct_array = Arc::new(StructArray::from(
            fields.into_iter().zip(arrays).collect::<Vec<(FieldRef, ArrayRef)>>(),
        )) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32, Type::String]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(&type_hint, &mut writer, &struct_array, &mut state).unwrap();
        assert!(writer.is_empty());
    }

    #[test]
    fn test_serialize_tuple_nullable() {
        let fields = vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, true),
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}2"), DataType::Utf8, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ];
        let struct_array = Arc::new(StructArray::from(
            fields.into_iter().zip(arrays).collect::<Vec<(FieldRef, ArrayRef)>>(),
        )) as ArrayRef;
        let type_hint = wrap_tuple(vec![
            Type::Nullable(Box::new(Type::Int32)),
            Type::Nullable(Box::new(Type::String)),
        ]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(&type_hint, &mut writer, &struct_array, &mut state).unwrap();
        let expected = vec![
            0, 1, 0, // Nullable(Int32) column: [1, null, 3]
            1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, // Nullable(String) column: ["a", null, "c"]
            0, 1, 0, 1, 97, 0, 1, 99,
        ];
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_nested_tuple() {
        let inner_fields = vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}2"), DataType::Int8, false),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let inner_arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["x", "y"])),
            Arc::new(Int8Array::from(vec![10, 20])),
        ];
        let inner_struct = Arc::new(StructArray::from(
            inner_fields
                .clone()
                .into_iter()
                .zip(inner_arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        ));
        let outer_fields = vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false),
            Field::new(
                format!("{TUPLE_FIELD_NAME_PREFIX}2"),
                DataType::Struct(inner_fields.into()),
                false,
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let outer_arrays: Vec<ArrayRef> =
            vec![Arc::new(Int32Array::from(vec![1, 2])), inner_struct];
        let struct_array = Arc::new(StructArray::from(
            outer_fields.into_iter().zip(outer_arrays).collect::<Vec<(FieldRef, ArrayRef)>>(),
        )) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32, Type::Tuple(vec![Type::String, Type::Int8])]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(&type_hint, &mut writer, &struct_array, &mut state).unwrap();
        let expected = vec![
            // Int32 column: [1, 2]
            1, 0, 0, 0, 2, 0, 0, 0,
            // Tuple(String, Int8) column: [("x", 10), ("y", 20)]
            // String column: ["x", "y"]
            1, 120, 1, 121, // Int8 column: [10, 20]
            10, 20,
        ];
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_single_element_tuple() {
        let fields = vec![(
            Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef,
        )];
        let struct_array = Arc::new(StructArray::from(fields.clone())) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Float64]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        serialize(&type_hint, &mut writer, &struct_array, &mut state).unwrap();
        let expected = vec![
            // Float64 column: [1.5, 2.5]
            0, 0, 0, 0, 0, 0, 248, 63, // 1.5 (0x3FF8000000000000)
            0, 0, 0, 0, 0, 0, 4, 64, // 2.5 (0x4004000000000000)
        ];
        assert_eq!(writer, expected);
    }

    #[test]
    fn test_serialize_invalid_array_type() {
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        let result = serialize(&type_hint, &mut writer, &column, &mut state);
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("Expected StructArray for Tuple type")
        ));
    }

    #[test]
    fn test_serialize_mismatched_field_count() {
        let fields = vec![(
            Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )];
        let struct_array = Arc::new(StructArray::from(fields)) as ArrayRef;
        let type_hint = wrap_tuple(vec![Type::Int32, Type::String]);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();

        let result = serialize(&type_hint, &mut writer, &struct_array, &mut state);
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("StructArray has 1 fields, but Tuple expects 2")
        ));
    }
}
