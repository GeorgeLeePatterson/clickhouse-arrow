use std::sync::Arc;

use arrow::array::{Array, ArrayRef, LargeListArray, ListArray, StructArray};
use arrow::datatypes::{DataType, Field};
use tokio::io::AsyncWriteExt;

use super::ClickHouseArrowSerializer;
use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    let Type::Nested(fields) = type_hint.strip_null() else {
        return Err(Error::ArrowSerialize(format!(
            "Expected Nested type for nested serialization, got {type_hint:?}",
        )));
    };

    let struct_array = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::ArrowSerialize("Expected StructArray for Nested type".into()))?;

    let DataType::Struct(arrow_fields) = data_type else {
        return Err(Error::ArrowSerialize("Expected Struct data type for Nested type".into()));
    };

    if fields.len() != arrow_fields.len() {
        return Err(Error::ArrowSerialize(format!(
            "Nested field count mismatch: nested={}, arrow={}",
            fields.len(),
            arrow_fields.len()
        )));
    }

    if fields.is_empty() {
        return Err(Error::ArrowSerialize(
            "Nested type must contain at least one field".to_string(),
        ));
    }

    let rows = struct_array.len();
    let mut shared_offsets: Option<Vec<u64>> = None;
    let mut tuple_types = Vec::with_capacity(fields.len());
    let mut tuple_fields = Vec::with_capacity(fields.len());
    let mut tuple_arrays = Vec::with_capacity(fields.len());

    for (i, ((nested_name, inner_type), arrow_field)) in
        fields.iter().zip(arrow_fields.iter()).enumerate()
    {
        if arrow_field.name() != nested_name {
            return Err(Error::ArrowSerialize(format!(
                "Nested field name mismatch at index {i}: nested='{nested_name}', arrow='{}'",
                arrow_field.name()
            )));
        }

        let nested_column = struct_array.column(i);
        let (offsets, values, item_type) = match arrow_field.data_type() {
            DataType::List(item) => {
                let list = nested_column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' expected ListArray, found {:?}",
                        nested_column.data_type()
                    ))
                })?;
                if list.len() != rows {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' row count mismatch: {} != {rows}",
                        list.len()
                    )));
                }
                if list.null_count() != 0 {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' contains null lists; Nested requires \
                         non-null arrays"
                    )));
                }
                (
                    list.value_offsets().iter().map(|offset| *offset as u64).collect::<Vec<_>>(),
                    list.values().clone(),
                    item.data_type().clone(),
                )
            }
            DataType::LargeList(item) => {
                let list =
                    nested_column.as_any().downcast_ref::<LargeListArray>().ok_or_else(|| {
                        Error::ArrowSerialize(format!(
                            "Nested field '{nested_name}' expected LargeListArray, found {:?}",
                            nested_column.data_type()
                        ))
                    })?;
                if list.len() != rows {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' row count mismatch: {} != {rows}",
                        list.len()
                    )));
                }
                if list.null_count() != 0 {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' contains null lists; Nested requires \
                         non-null arrays"
                    )));
                }
                let mut offsets = Vec::with_capacity(list.value_offsets().len());
                for offset in list.value_offsets() {
                    let offset = u64::try_from(*offset).map_err(|_| {
                        Error::ArrowSerialize(format!(
                            "Nested field '{nested_name}' contains negative LargeList offset: \
                             {offset}"
                        ))
                    })?;
                    offsets.push(offset);
                }
                (offsets, list.values().clone(), item.data_type().clone())
            }
            other => {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' expected List/LargeList type, got {other:?}"
                )));
            }
        };

        if offsets.len() != rows + 1 {
            return Err(Error::ArrowSerialize(format!(
                "Nested field '{nested_name}' offset length mismatch: {} != {}",
                offsets.len(),
                rows + 1
            )));
        }

        if let Some(shared_offsets) = shared_offsets.as_ref() {
            if shared_offsets != &offsets {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' offsets differ across Nested children"
                )));
            }
        } else {
            shared_offsets = Some(offsets);
        }

        tuple_types.push(inner_type.clone());
        tuple_fields.push(Field::new(nested_name, item_type, inner_type.is_nullable()));
        tuple_arrays.push(values);
    }

    let shared_offsets = shared_offsets.expect("Nested fields are checked as non-empty");
    let total_values = shared_offsets.last().copied().unwrap_or_default() as usize;
    for (i, value_array) in tuple_arrays.iter().enumerate() {
        if value_array.len() != total_values {
            return Err(Error::ArrowSerialize(format!(
                "Nested child '{}' values length mismatch: {} != {total_values}",
                fields[i].0,
                value_array.len()
            )));
        }
    }

    for offset in shared_offsets.iter().skip(1) {
        writer.write_u64_le(*offset).await?;
    }

    let tuple_type = Type::Tuple(tuple_types);
    let tuple_array: ArrayRef = Arc::new(StructArray::new(tuple_fields.into(), tuple_arrays, None));
    tuple_type.serialize_async(writer, &tuple_array, tuple_array.data_type(), state).await?;

    Ok(())
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    let Type::Nested(fields) = type_hint.strip_null() else {
        return Err(Error::ArrowSerialize(format!(
            "Expected Nested type for nested serialization, got {type_hint:?}",
        )));
    };

    let struct_array = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::ArrowSerialize("Expected StructArray for Nested type".into()))?;

    let DataType::Struct(arrow_fields) = data_type else {
        return Err(Error::ArrowSerialize("Expected Struct data type for Nested type".into()));
    };

    if fields.len() != arrow_fields.len() {
        return Err(Error::ArrowSerialize(format!(
            "Nested field count mismatch: nested={}, arrow={}",
            fields.len(),
            arrow_fields.len()
        )));
    }

    if fields.is_empty() {
        return Err(Error::ArrowSerialize(
            "Nested type must contain at least one field".to_string(),
        ));
    }

    let rows = struct_array.len();
    let mut shared_offsets: Option<Vec<u64>> = None;
    let mut tuple_types = Vec::with_capacity(fields.len());
    let mut tuple_fields = Vec::with_capacity(fields.len());
    let mut tuple_arrays = Vec::with_capacity(fields.len());

    for (i, ((nested_name, inner_type), arrow_field)) in
        fields.iter().zip(arrow_fields.iter()).enumerate()
    {
        if arrow_field.name() != nested_name {
            return Err(Error::ArrowSerialize(format!(
                "Nested field name mismatch at index {i}: nested='{nested_name}', arrow='{}'",
                arrow_field.name()
            )));
        }

        let nested_column = struct_array.column(i);
        let (offsets, values, item_type) = match arrow_field.data_type() {
            DataType::List(item) => {
                let list = nested_column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' expected ListArray, found {:?}",
                        nested_column.data_type()
                    ))
                })?;
                if list.len() != rows {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' row count mismatch: {} != {rows}",
                        list.len()
                    )));
                }
                if list.null_count() != 0 {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' contains null lists; Nested requires \
                         non-null arrays"
                    )));
                }
                (
                    list.value_offsets().iter().map(|offset| *offset as u64).collect::<Vec<_>>(),
                    list.values().clone(),
                    item.data_type().clone(),
                )
            }
            DataType::LargeList(item) => {
                let list =
                    nested_column.as_any().downcast_ref::<LargeListArray>().ok_or_else(|| {
                        Error::ArrowSerialize(format!(
                            "Nested field '{nested_name}' expected LargeListArray, found {:?}",
                            nested_column.data_type()
                        ))
                    })?;
                if list.len() != rows {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' row count mismatch: {} != {rows}",
                        list.len()
                    )));
                }
                if list.null_count() != 0 {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' contains null lists; Nested requires \
                         non-null arrays"
                    )));
                }
                let mut offsets = Vec::with_capacity(list.value_offsets().len());
                for offset in list.value_offsets() {
                    let offset = u64::try_from(*offset).map_err(|_| {
                        Error::ArrowSerialize(format!(
                            "Nested field '{nested_name}' contains negative LargeList offset: \
                             {offset}"
                        ))
                    })?;
                    offsets.push(offset);
                }
                (offsets, list.values().clone(), item.data_type().clone())
            }
            other => {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' expected List/LargeList type, got {other:?}"
                )));
            }
        };

        if offsets.len() != rows + 1 {
            return Err(Error::ArrowSerialize(format!(
                "Nested field '{nested_name}' offset length mismatch: {} != {}",
                offsets.len(),
                rows + 1
            )));
        }

        if let Some(shared_offsets) = shared_offsets.as_ref() {
            if shared_offsets != &offsets {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' offsets differ across Nested children"
                )));
            }
        } else {
            shared_offsets = Some(offsets);
        }

        tuple_types.push(inner_type.clone());
        tuple_fields.push(Field::new(nested_name, item_type, inner_type.is_nullable()));
        tuple_arrays.push(values);
    }

    let shared_offsets = shared_offsets.expect("Nested fields are checked as non-empty");
    let total_values = shared_offsets.last().copied().unwrap_or_default() as usize;
    for (i, value_array) in tuple_arrays.iter().enumerate() {
        if value_array.len() != total_values {
            return Err(Error::ArrowSerialize(format!(
                "Nested child '{}' values length mismatch: {} != {total_values}",
                fields[i].0,
                value_array.len()
            )));
        }
    }

    for offset in shared_offsets.iter().skip(1) {
        writer.put_u64_le(*offset);
    }

    let tuple_type = Type::Tuple(tuple_types);
    let tuple_array: ArrayRef = Arc::new(StructArray::new(tuple_fields.into(), tuple_arrays, None));
    tuple_type.serialize(writer, &tuple_array, tuple_array.data_type(), state)?;

    Ok(())
}
