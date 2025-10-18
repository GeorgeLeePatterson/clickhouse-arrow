/// Deserialization logic for `ClickHouse` `Tuple` types into Arrow `StructArray`.
///
/// This module provides a function to deserialize `ClickHouse`’s native format for `Tuple`
/// types into an Arrow `StructArray`, which represents a collection of fields.
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;

use super::ClickHouseArrowDeserializer;
use crate::arrow::builder::TypedBuilder;
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::{Error, Result, Type};

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
/// `Error` if deserialization fails.
///
/// # Errors
/// - Returns `ArrowDeserialize` if an inner type is unsupported or the data is malformed.
/// - Returns `Io` if reading from the reader fails.
pub(super) async fn deserialize_async<R: ClickHouseRead>(
    inner_types: &[Type],
    builder: &mut TypedBuilder,
    data_type: &DataType,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    // Read each field’s data
    let DataType::Struct(fields) = data_type else {
        return Err(Error::ArrowDeserialize(format!("Unsupported tuple datatype: {data_type:?}")));
    };

    let TypedBuilder::Tuple(builders) = builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected tuple builder: {}",
            builder.as_ref()
        )));
    };

    if rows == 0 {
        let arrays = vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())),
        ];
        return Ok(Arc::new(StructArray::try_new_with_length(fields.clone(), arrays, None, 0)?));
    }

    let mut arrays = Vec::with_capacity(inner_types.len());
    let field_types = inner_types.iter().zip(fields.iter());
    for (b, (inner_type, field)) in builders.iter_mut().zip(field_types) {
        let data_type = field.data_type();
        arrays.push(
            inner_type.deserialize_arrow_async(b, reader, data_type, rows, &[], rbuffer).await?,
        );
    }
    let null_buffer = if nulls.is_empty() {
        None
    } else {
        Some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()))
    };
    Ok(Arc::new(StructArray::new(fields.clone(), arrays, null_buffer)))
}

#[allow(dead_code)] // TODO: remove once synchronous Arrow path is fully retired
pub(super) fn deserialize<R: ClickHouseBytesRead>(
    builders: &mut [TypedBuilder],
    reader: &mut R,
    inner: &[Type],
    data_type: &DataType,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = data_type else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected datatype for tuple: {data_type:?}",
        )));
    };

    if rows == 0 {
        let arrays = vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<String>::new())),
        ];
        return Ok(Arc::new(StructArray::try_new_with_length(fields.clone(), arrays, None, 0)?));
    }

    let mut arrays = Vec::with_capacity(inner.len());
    let field_types = inner.iter().zip(fields.iter());
    for (b, (inner_type, field)) in builders.iter_mut().zip(field_types) {
        let data_type = field.data_type();
        arrays.push(inner_type.deserialize_arrow(b, reader, data_type, rows, &[], rbuffer)?);
    }
    let null_buffer = if nulls.is_empty() {
        None
    } else {
        Some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()))
    };
    Ok(Arc::new(StructArray::new(fields.clone(), arrays, null_buffer)))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{Array, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use crate::arrow::types::{TUPLE_FIELD_NAME_PREFIX, ch_to_arrow_type};
    use crate::native::types::Type;
    use crate::{ArrowOptions, Error};

    fn create_inner_fields(inner: &[Type]) -> Fields {
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        inner
            .iter()
            .map(|i| ch_to_arrow_type(i, opts).unwrap())
            .enumerate()
            .map(|(i, (d, nil))| Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}{i}"), d, nil))
            .collect::<Fields>()
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

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builder =
            TypedBuilder::try_new(&Type::Tuple(inner_types.clone()), &data_type).unwrap();
        let result = deserialize_async(
            &inner_types,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
        .await
        .expect("Failed to deserialize Tuple(Int32, String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(fields, &inner_fields);
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

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builder =
            TypedBuilder::try_new(&Type::Tuple(inner_types.clone()), &data_type).unwrap();
        let result = deserialize_async(
            &inner_types,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
        .await
        .inspect_err(|error| {
            eprintln!("Error reading data: {error:?}");
            eprintln!("Currently read: {reader:?}");
        })
        .expect("Failed to deserialize Tuple(Nullable(Int32), String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(fields, &inner_fields);
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

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builder =
            TypedBuilder::try_new(&Type::Tuple(inner_types.clone()), &data_type).unwrap();
        let result = deserialize_async(
            &inner_types,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
        .await
        .expect("Failed to deserialize nullable Tuple(Int32, String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(fields, &inner_fields,);
        //     &Fields::from(vec![
        //         Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
        //         Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
        //     ])
        // );
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

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builder =
            TypedBuilder::try_new(&Type::Tuple(inner_types.clone()), &data_type).unwrap();
        let result = deserialize_async(
            &inner_types,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
        .await
        .expect("Failed to deserialize Tuple(Int32, Tuple(String, Int32))");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(fields, &inner_fields,);
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

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builder =
            TypedBuilder::try_new(&Type::Tuple(inner_types.clone()), &data_type).unwrap();
        let result = deserialize_async(
            &inner_types,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
        .await
        .expect("Failed to deserialize Tuple(Int32, String) with zero rows");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(
            fields,
            &inner_fields,
            // &Fields::from(vec![
            //     Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
            //     Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            // ])
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

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builder =
            TypedBuilder::try_new(&Type::Tuple(inner_types.clone()), &data_type).unwrap();
        let result = deserialize_async(
            &inner_types,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
        .await;
        assert!(matches!(result, Err(Error::ArrowDeserialize(_))));
    }
}

#[cfg(test)]
mod tests_sync {
    use std::io::Cursor;

    use arrow::array::{Array, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use crate::arrow::types::{TUPLE_FIELD_NAME_PREFIX, ch_to_arrow_type};
    use crate::native::types::Type;
    use crate::{ArrowOptions, Error};

    fn create_inner_fields(inner: &[Type]) -> Fields {
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        inner
            .iter()
            .map(|i| ch_to_arrow_type(i, opts).unwrap())
            .enumerate()
            .map(|(i, (d, nil))| Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}{i}"), d, nil))
            .collect::<Fields>()
    }

    #[test]
    fn test_deserialize_tuple_int32_string() {
        let inner_types = vec![Type::Int32, Type::String];
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Field 1: Int32 [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // Field 2: String ["a", "bb", "ccc"]
            1, b'a', 2, b'b', b'b', 3, b'c', b'c', b'c',
        ];
        let mut reader = Cursor::new(input);

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builders = inner_types
            .iter()
            .zip(inner_fields.iter())
            .map(|(t, f)| TypedBuilder::try_new(t, f.data_type()).unwrap())
            .collect::<Vec<_>>();
        let result = deserialize(
            &mut builders,
            &mut reader,
            &inner_types,
            &data_type,
            rows,
            &nulls,
            &mut vec![],
        )
        .expect("Failed to deserialize Tuple(Int32, String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(fields, &inner_fields);
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

    #[test]
    fn test_deserialize_tuple_nullable_int32_string() {
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

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builders = inner_types
            .iter()
            .zip(inner_fields.iter())
            .map(|(t, f)| TypedBuilder::try_new(t, f.data_type()).unwrap())
            .collect::<Vec<_>>();
        let result = deserialize(
            &mut builders,
            &mut reader,
            &inner_types,
            &data_type,
            rows,
            &nulls,
            &mut vec![],
        )
        .inspect_err(|error| {
            eprintln!("Error reading data: {error:?}");
            eprintln!("Currently read: {reader:?}");
        })
        .expect("Failed to deserialize Tuple(Nullable(Int32), String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(fields, &inner_fields);
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

    #[test]
    fn test_deserialize_nullable_tuple_int32_string() {
        let inner_types = vec![Type::Int32, Type::String];
        let rows = 3;
        let nulls = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Field 1: Int32 [1, 2, 3]
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // Field 2: String ["a", "b", "c"]
            1, b'a', 1, b'b', 1, b'c',
        ];
        let mut reader = Cursor::new(input);

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builders = inner_types
            .iter()
            .zip(inner_fields.iter())
            .map(|(t, f)| TypedBuilder::try_new(t, f.data_type()).unwrap())
            .collect::<Vec<_>>();
        let result = deserialize(
            &mut builders,
            &mut reader,
            &inner_types,
            &data_type,
            rows,
            &nulls,
            &mut vec![],
        )
        .expect("Failed to deserialize nullable Tuple(Int32, String)");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(fields, &inner_fields,);
        //     &Fields::from(vec![
        //         Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
        //         Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
        //     ])
        // );
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

    #[test]
    fn test_deserialize_tuple_nested() {
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

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builders = inner_types
            .iter()
            .zip(inner_fields.iter())
            .map(|(t, f)| TypedBuilder::try_new(t, f.data_type()).unwrap())
            .collect::<Vec<_>>();
        let result = deserialize(
            &mut builders,
            &mut reader,
            &inner_types,
            &data_type,
            rows,
            &nulls,
            &mut vec![],
        )
        .expect("Failed to deserialize Tuple(Int32, Tuple(String, Int32))");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(
            fields,
            &inner_fields,
            // &Fields::from(vec![
            //     Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
            //     Field::new(
            //         format!("{TUPLE_FIELD_NAME_PREFIX}1"),
            //         DataType::Struct(Fields::from(vec![
            //             Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Utf8,
            // false),             Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"),
            // DataType::Int32, false),         ])),
            //         false
            //     ),
            // ])
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

    #[test]
    fn test_deserialize_tuple_zero_rows() {
        let inner_types = vec![Type::Int32, Type::String];
        let rows = 0;
        let nulls = vec![];
        let input = vec![]; // No data for zero rows
        let mut reader = Cursor::new(input);

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builders = inner_types
            .iter()
            .zip(inner_fields.iter())
            .map(|(t, f)| TypedBuilder::try_new(t, f.data_type()).unwrap())
            .collect::<Vec<_>>();
        let result = deserialize(
            &mut builders,
            &mut reader,
            &inner_types,
            &data_type,
            rows,
            &nulls,
            &mut vec![],
        )
        .expect("Failed to deserialize Tuple(Int32, String) with zero rows");
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_array.fields();
        let arrays = struct_array.columns();

        assert_eq!(fields, &inner_fields,);
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

    #[test]
    fn test_deserialize_tuple_invalid_inner_type() {
        let inner_types = vec![Type::Enum16(vec![]), Type::String]; // Enum16 may be unsupported
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Field 1: Invalid Enum16 data
            1, 0, 2, 0, 3, 0, // Field 2: String ["a", "b", "c"]
            1, b'a', 1, b'b', 1, b'c',
        ];
        let mut reader = Cursor::new(input);

        let inner_fields = create_inner_fields(&inner_types);
        let data_type = DataType::Struct(inner_fields.clone());

        let mut builders = inner_types
            .iter()
            .zip(inner_fields.iter())
            .map(|(t, f)| TypedBuilder::try_new(t, f.data_type()).unwrap())
            .collect::<Vec<_>>();
        let result = deserialize(
            &mut builders,
            &mut reader,
            &inner_types,
            &data_type,
            rows,
            &nulls,
            &mut vec![],
        );
        assert!(matches!(result, Err(Error::ArrowDeserialize(_))));
    }
}
