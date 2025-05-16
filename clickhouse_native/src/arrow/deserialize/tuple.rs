/// Deserialization logic for `ClickHouse` `Tuple` types into Arrow `StructArray`.
///
/// This module provides a function to deserialize ClickHouse’s native format for `Tuple` types
/// into an Arrow `StructArray`, which represents a collection of fields.
use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Fields};

use super::ClickhouseArrowDeserializer;
use crate::arrow::types::TUPLE_FIELD_NAME_PREFIX;
use crate::formats::DeserializerState;
use crate::io::ClickhouseRead;
use crate::{Result, Type};

/// Deserializes a `ClickHouse` `Tuple` type into an Arrow `StructArray`.
///
/// Reads the data for each field in the tuple, constructing a `StructArray` with the corresponding
/// inner types.
///
/// # Arguments
/// - `inner_types`: The `ClickHouse` types of the tuple’s fields.
/// - `reader`: The async reader providing the `ClickHouse` native format data.
/// - `rows`: The number of tuples to deserialize.
/// - `nulls`: A slice indicating null values (`1` for null, `0` for non-null).
/// - `state`: A mutable `DeserializerState` for deserialization context.
///
/// # Returns
/// A `Result` containing the deserialized `StructArray` as an `ArrayRef` or a
/// `ClickhouseNativeError` if deserialization fails.
///
/// # Errors
/// - Returns `ArrowDeserialize` if an inner type is unsupported or the data is malformed.
/// - Returns `Io` if reading from the reader fails.
pub(super) async fn deserialize<R: ClickhouseRead>(
    inner_types: &[Type],
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    state: &mut DeserializerState,
) -> Result<ArrayRef> {
    // Read each field’s data
    let mut fields = Vec::with_capacity(inner_types.len());
    let mut arrays = Vec::with_capacity(inner_types.len());
    for (i, inner_type) in inner_types.iter().enumerate() {
        // TODO: Can the field be determine without calling `arrow_type`?
        let (data_type, is_nullable) = inner_type.arrow_type(state.options)?;
        let array = inner_type.deserialize(reader, rows, &[], state).await?;
        fields.push(Arc::new(Field::new(
            format!("{TUPLE_FIELD_NAME_PREFIX}{i}"),
            data_type,
            is_nullable,
        )));
        arrays.push(array);
    }

    // Construct StructArray
    let null_buffer = if nulls.is_empty() {
        None
    } else {
        Some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()))
    };

    Ok(Arc::new(StructArray::new(
        Fields::from(fields.iter().map(|f| f.as_ref().clone()).collect::<Vec<_>>()),
        arrays,
        null_buffer,
    )) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{Array, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use crate::native::types::Type;
    use crate::{ArrowOptions, ClickhouseNativeError};

    // Helps keep all strings_as_strings
    fn get_state() -> DeserializerState {
        DeserializerState::default()
            .with_arrow_options(ArrowOptions::default().with_strings_as_strings(true))
    }

    #[tokio::test]
    async fn test_deserialize_tuple_int32_string() {
        let inner_types = vec![Type::Int32, Type::String];
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Field 1: Int32 [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // Field 2: String ["a", "bb", "ccc"]
            1, b'a', 2, b'b', b'b', 3, b'c', b'c', b'c',
        ];
        let mut reader = Cursor::new(input);
        let mut state = get_state();

        let result = deserialize(&inner_types, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Tuple(Int32, String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(
            fields,
            &Fields::from(vec![
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            ])
        );
        assert_eq!(
            arrays[0].as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![1, 2, 3])
        );
        assert_eq!(
            arrays[1].as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a", "bb", "ccc"])
        );
        assert_eq!(struct_array.nulls(), None);
    }

    #[tokio::test]
    async fn test_deserialize_tuple_nullable_int32_string() {
        let inner_types = vec![Type::Nullable(Box::new(Type::Int32)), Type::String];
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Field 1: Nullable(Int32) [Some(1), None, Some(3)]
            0, 1, 0, // Null mask: [1, 0, 1]
            1, 0, 0, 0, // Data: 1
            0, 0, 0, 0, // Data: None
            3, 0, 0, 0, // Data: 3
            1, b'a', 1, b'b', 1, b'c', // Field 2: String ["a", "b", "c"]
        ];
        let mut reader = Cursor::new(input);
        let mut state = get_state();

        let result = deserialize(&inner_types, &mut reader, rows, &nulls, &mut state)
            .await
            .inspect_err(|error| {
                eprintln!("Error reading data: {error:?}");
                eprintln!("Currently read: {reader:?}");
            })
            .expect("Failed to deserialize Tuple(Nullable(Int32), String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(
            fields,
            &Fields::from(vec![
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, true),
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            ])
        );
        assert_eq!(
            arrays[0].as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(1), None, Some(3)])
        );
        assert_eq!(
            arrays[1].as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a", "b", "c"])
        );
        assert_eq!(struct_array.nulls(), None);
    }

    #[tokio::test]
    async fn test_deserialize_nullable_tuple_int32_string() {
        let inner_types = vec![Type::Int32, Type::String];
        let rows = 3;
        let nulls = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Field 1: Int32 [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // Field 2: String ["a", "b", "c"]
            1, b'a', 1, b'b', 1, b'c',
        ];
        let mut reader = Cursor::new(input);
        let mut state = get_state();

        let result = deserialize(&inner_types, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize nullable Tuple(Int32, String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(
            fields,
            &Fields::from(vec![
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            ])
        );
        assert_eq!(
            arrays[0].as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![1, 2, 3])
        );
        assert_eq!(
            arrays[1].as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a", "b", "c"])
        );
        assert_eq!(
            struct_array.nulls().unwrap().iter().collect::<Vec<bool>>(),
            vec![true, false, true] // 0=not null, 1=null
        );
    }

    #[tokio::test]
    async fn test_deserialize_tuple_nested() {
        let inner_types = vec![Type::Int32, Type::Tuple(vec![Type::String, Type::Int32])];
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Field 1: Int32 [1, 2]
            1, 0, 0, 0, 2, 0, 0, 0,
            // Field 2: Tuple(String, Int32) [("a", 10), ("b", 20)]
            // Inner field 1: String ["a", "b"]
            1, b'a', 1, b'b', // Inner field 2: Int32 [10, 20]
            10, 0, 0, 0, 20, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = get_state();

        let result = deserialize(&inner_types, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Tuple(Int32, Tuple(String, Int32))");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(
            fields,
            &Fields::from(vec![
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
                Field::new(
                    format!("{TUPLE_FIELD_NAME_PREFIX}1"),
                    DataType::Struct(Fields::from(vec![
                        Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Utf8, false),
                        Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Int32, false),
                    ])),
                    false
                ),
            ])
        );
        assert_eq!(
            arrays[0].as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![1, 2])
        );
        let inner_struct = arrays[1].as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            inner_struct.column(0).as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a", "b"])
        );
        assert_eq!(
            inner_struct.column(1).as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![10, 20])
        );
        assert_eq!(struct_array.nulls(), None);
    }

    #[tokio::test]
    async fn test_deserialize_tuple_zero_rows() {
        let inner_types = vec![Type::Int32, Type::String];
        let rows = 0;
        let nulls = vec![];
        let input = vec![]; // No data for zero rows
        let mut reader = Cursor::new(input);
        let mut state = get_state();

        let result = deserialize(&inner_types, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Tuple(Int32, String) with zero rows");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(
            fields,
            &Fields::from(vec![
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            ])
        );
        assert_eq!(
            arrays[0].as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(Vec::<i32>::new())
        );
        assert_eq!(
            arrays[1].as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(Vec::<String>::new())
        );
        assert_eq!(struct_array.nulls(), None);
    }

    #[tokio::test]
    async fn test_deserialize_tuple_invalid_inner_type() {
        let inner_types = vec![Type::Enum16(vec![]), Type::String]; // Enum16 may be unsupported
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Field 1: Invalid Enum16 data
            1, 0, 2, 0, 3, 0, // Field 2: String ["a", "b", "c"]
            1, b'a', 1, b'b', 1, b'c',
        ];
        let mut reader = Cursor::new(input);
        let mut state = get_state();

        let result = deserialize(&inner_types, &mut reader, rows, &nulls, &mut state).await;
        assert!(matches!(result, Err(ClickhouseNativeError::ArrowDeserialize(_))));
    }
}
