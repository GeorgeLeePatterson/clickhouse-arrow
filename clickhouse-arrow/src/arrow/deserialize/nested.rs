use std::sync::Arc;

use arrow::array::{ArrayRef, LargeListArray, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use tokio::io::AsyncReadExt;

use super::{ArrowFieldCtx, list, tuple};
use crate::arrow::builder::TypedBuilder;
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

#[expect(clippy::cast_possible_truncation)]
pub(super) async fn deserialize<R: ClickHouseRead>(
    fields: &[(String, Type)],
    builder: &mut TypedBuilder,
    data_type: &DataType,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    ctx: &mut ArrowFieldCtx<'_>,
) -> Result<ArrayRef> {
    let DataType::Struct(arrow_fields) = data_type else {
        return Err(Error::ArrowDeserialize(format!(
            "Nested expects Struct datatype, got {data_type:?}"
        )));
    };

    let tuple_builder_len = match builder {
        TypedBuilder::Tuple(builders) => builders.len(),
        _ => {
            return Err(Error::ArrowDeserialize(format!(
                "Unexpected Nested builder: {}",
                builder.as_ref()
            )));
        }
    };

    if fields.len() != arrow_fields.len() || tuple_builder_len != fields.len() {
        return Err(Error::ArrowDeserialize(format!(
            "Nested field count mismatch: type={}, arrow={}, builder={}",
            fields.len(),
            arrow_fields.len(),
            tuple_builder_len
        )));
    }

    let offset_bytes = list::bulk_offsets!(tokio; reader, ctx.row_buffer, rows);
    let offsets_u64 = bytemuck::cast_slice::<u8, u64>(&ctx.row_buffer[..offset_bytes]).to_vec();
    if offsets_u64.len() != rows + 1 {
        return Err(Error::ArrowDeserialize(format!(
            "Nested offset length mismatch: {} != {}",
            offsets_u64.len(),
            rows + 1
        )));
    }
    let total_values = offsets_u64.last().copied().unwrap_or_default() as usize;

    let mut tuple_types = Vec::with_capacity(fields.len());
    let mut tuple_fields = Vec::with_capacity(fields.len());
    for ((nested_name, inner_type), arrow_field) in fields.iter().zip(arrow_fields.iter()) {
        if arrow_field.name() != nested_name {
            return Err(Error::ArrowDeserialize(format!(
                "Nested field name mismatch: expected '{nested_name}', got '{}'",
                arrow_field.name()
            )));
        }

        let list_item_type = match arrow_field.data_type() {
            DataType::List(item) | DataType::LargeList(item) => item.data_type().clone(),
            other => {
                return Err(Error::ArrowDeserialize(format!(
                    "Nested field '{nested_name}' expected List/LargeList datatype, got {other:?}"
                )));
            }
        };

        tuple_types.push(inner_type.clone());
        tuple_fields.push(Field::new(nested_name, list_item_type, inner_type.is_nullable()));
    }

    let tuple_data_type = DataType::Struct(tuple_fields.into());
    let tuple_values =
        tuple::deserialize(&tuple_types, builder, &tuple_data_type, reader, total_values, &[], ctx)
            .await?;
    let tuple_values = tuple_values.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        Error::ArrowDeserialize("Nested tuple payload is not a StructArray".into())
    })?;

    let offsets_i32 = offsets_u64
        .iter()
        .map(|offset| {
            i32::try_from(*offset).map_err(|_| {
                Error::ArrowDeserialize(format!("Nested offset exceeds Int32 list limit: {offset}"))
            })
        })
        .collect::<Result<ScalarBuffer<i32>>>()?;
    let offsets_i64 = offsets_u64
        .iter()
        .map(|offset| {
            i64::try_from(*offset).map_err(|_| {
                Error::ArrowDeserialize(format!("Nested offset exceeds Int64 list limit: {offset}"))
            })
        })
        .collect::<Result<ScalarBuffer<i64>>>()?;

    let mut arrays = Vec::with_capacity(fields.len());
    for (idx, arrow_field) in arrow_fields.iter().enumerate() {
        let values = tuple_values.column(idx).clone();
        if values.len() != total_values {
            return Err(Error::ArrowDeserialize(format!(
                "Nested child '{}' length mismatch: {} != {total_values}",
                fields[idx].0,
                values.len()
            )));
        }

        let array: ArrayRef = match arrow_field.data_type() {
            DataType::List(item) => Arc::new(ListArray::new(
                item.clone(),
                OffsetBuffer::new(offsets_i32.clone()),
                values,
                None,
            )),
            DataType::LargeList(item) => Arc::new(LargeListArray::new(
                item.clone(),
                OffsetBuffer::new(offsets_i64.clone()),
                values,
                None,
            )),
            other => {
                return Err(Error::ArrowDeserialize(format!(
                    "Nested field '{}' expected List/LargeList datatype, got {other:?}",
                    fields[idx].0
                )));
            }
        };
        arrays.push(array);
    }

    let null_buffer = (!nulls.is_empty())
        .then_some(NullBuffer::from(nulls.iter().map(|value| *value == 0).collect::<Vec<_>>()));
    Ok(Arc::new(StructArray::new(arrow_fields.clone(), arrays, null_buffer)))
}
