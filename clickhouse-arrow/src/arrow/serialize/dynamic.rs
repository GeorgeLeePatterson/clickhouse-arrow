use arrow::array::{Array, ArrayRef, BooleanArray, UnionArray};
use arrow::compute::filter;
use arrow::datatypes::DataType;
use tokio::io::AsyncWriteExt;

use super::ClickHouseArrowSerializer;
use crate::formats::{DynamicPrefixState, SerializerState};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

const DYNAMIC_TYPE_ID_UNSET: u16 = u16::MAX;

#[derive(Debug, Clone)]
struct DynamicUnionField {
    logical_type: Type,
    type_id:      i8,
}

#[derive(Debug, Clone)]
struct DynamicTypeIdLookup {
    by_type_id: [u16; 256],
}

impl DynamicTypeIdLookup {
    fn new() -> Self { Self { by_type_id: [DYNAMIC_TYPE_ID_UNSET; 256] } }

    #[expect(clippy::cast_sign_loss)]
    #[inline]
    fn slot(type_id: i8) -> usize { (i16::from(type_id) + 128) as usize }

    fn insert(&mut self, type_id: i8, field_idx: usize) -> Result<()> {
        let slot = Self::slot(type_id);
        if self.by_type_id[slot] != DYNAMIC_TYPE_ID_UNSET {
            return Err(Error::ArrowSerialize(format!(
                "Dynamic union has duplicate type id {type_id}"
            )));
        }

        let field_idx = u16::try_from(field_idx).map_err(|_| {
            Error::ArrowSerialize(format!(
                "Dynamic union child index {field_idx} exceeds supported lookup range"
            ))
        })?;
        self.by_type_id[slot] = field_idx;
        Ok(())
    }

    fn resolve(&self, type_id: i8) -> Result<usize> {
        let slot = Self::slot(type_id);
        let field_idx = self.by_type_id[slot];
        if field_idx == DYNAMIC_TYPE_ID_UNSET {
            return Err(Error::ArrowSerialize(format!(
                "Dynamic union row references unknown type id {type_id}"
            )));
        }
        Ok(usize::from(field_idx))
    }
}

fn parse_dynamic_union_schema(
    data_type: &DataType,
    dynamic_prefix: DynamicPrefixState,
) -> Result<(Vec<DynamicUnionField>, DynamicTypeIdLookup)> {
    let DataType::Union(fields, _) = data_type else {
        return Err(Error::ArrowSerialize(format!(
            "Dynamic strict mode expects Arrow Union, found {data_type:?}"
        )));
    };

    if dynamic_prefix.flattened_types.len() != fields.len() {
        return Err(Error::ArrowSerialize(format!(
            "Dynamic union child count mismatch between prefix metadata and Arrow schema: {} != {}",
            dynamic_prefix.flattened_types.len(),
            fields.len()
        )));
    }

    let mut field_meta = Vec::with_capacity(fields.len());
    let mut by_type_id = DynamicTypeIdLookup::new();
    for ((type_id, _field), logical_type) in fields.iter().zip(dynamic_prefix.flattened_types) {
        by_type_id.insert(type_id, field_meta.len())?;
        field_meta.push(DynamicUnionField { logical_type, type_id });
    }

    if field_meta.is_empty() {
        return Err(Error::ArrowSerialize(
            "Dynamic strict union must contain at least one child field".to_string(),
        ));
    }

    Ok((field_meta, by_type_id))
}

#[inline]
fn dynamic_index_width(total_types: usize) -> usize {
    // ClickHouse selects the index integer width from the number of non-null Dynamic types.
    // The null discriminator is encoded as `total_types`.
    if u8::try_from(total_types).is_ok() {
        1
    } else if u16::try_from(total_types).is_ok() {
        2
    } else if u32::try_from(total_types).is_ok() {
        4
    } else {
        8
    }
}

#[inline]
async fn write_dynamic_index_async<W: ClickHouseWrite>(
    writer: &mut W,
    width: usize,
    index: usize,
) -> Result<()> {
    match width {
        1 => writer.write_u8(index as u8).await?,
        2 => writer.write_u16_le(index as u16).await?,
        4 => writer.write_u32_le(index as u32).await?,
        8 => writer.write_u64_le(index as u64).await?,
        _ => {
            return Err(Error::ArrowSerialize(format!(
                "invalid Dynamic discriminator width: {width}"
            )));
        }
    }
    Ok(())
}

#[inline]
fn write_dynamic_index_sync<W: ClickHouseBytesWrite>(
    writer: &mut W,
    width: usize,
    index: usize,
) -> Result<()> {
    match width {
        1 => writer.put_u8(index as u8),
        2 => writer.put_u16_le(index as u16),
        4 => writer.put_u32_le(index as u32),
        8 => writer.put_u64_le(index as u64),
        _ => {
            return Err(Error::ArrowSerialize(format!(
                "invalid Dynamic discriminator width: {width}"
            )));
        }
    }
    Ok(())
}

pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    if !matches!(type_.strip_null(), Type::Dynamic { .. }) {
        return Err(Error::ArrowSerialize(format!("Dynamic serialize type unsupported: {type_}")));
    }

    let Some(union) = column.as_any().downcast_ref::<UnionArray>() else {
        return Err(Error::ArrowSerialize("Expected UnionArray for Dynamic serialization".into()));
    };

    let dynamic_prefix = state.take_dynamic_prefix().ok_or_else(|| {
        Error::ArrowSerialize("Dynamic serialization requires prefix metadata".to_string())
    })?;

    let (fields, by_type_id) = parse_dynamic_union_schema(data_type, dynamic_prefix)?;
    let total_types = fields.len();
    let null_discriminator = total_types;
    let discriminator_width = dynamic_index_width(total_types);
    let mut source_counts = vec![0_usize; total_types];
    let mut source_keep_mask: Option<Vec<Option<Vec<bool>>>> = None;

    for row in 0..union.len() {
        let child_type_id = union.type_id(row);
        let source_idx = by_type_id.resolve(child_type_id)?;
        let value_offset = union.value_offset(row);
        let child = union.child(child_type_id);
        if source_counts[source_idx] <= value_offset {
            source_counts[source_idx] = value_offset + 1;
            if let Some(masks) = source_keep_mask.as_mut()
                && let Some(mask) = masks[source_idx].as_mut()
            {
                mask.resize(value_offset + 1, true);
            }
        }

        if child.is_null(value_offset) {
            write_dynamic_index_async(writer, discriminator_width, null_discriminator).await?;
            let masks = source_keep_mask.get_or_insert_with(|| vec![None; total_types]);
            let mask =
                masks[source_idx].get_or_insert_with(|| vec![true; source_counts[source_idx]]);
            if mask.len() <= value_offset {
                mask.resize(value_offset + 1, true);
            }
            mask[value_offset] = false;
            continue;
        }

        write_dynamic_index_async(writer, discriminator_width, source_idx).await?;
        if let Some(masks) = source_keep_mask.as_mut()
            && let Some(mask) = masks[source_idx].as_mut()
        {
            if mask.len() <= value_offset {
                mask.resize(value_offset + 1, true);
            }
            mask[value_offset] = true;
        }
    }

    for (source_idx, field) in fields.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let mut values = union.child(field.type_id).clone();
        if source_rows < values.len() {
            values = values.slice(0, source_rows);
        } else if source_rows > values.len() {
            return Err(Error::ArrowSerialize(format!(
                "Dynamic child '{}' row count mismatch: {} > {}",
                field.logical_type,
                source_rows,
                values.len()
            )));
        }

        if let Some(keep_mask) = source_keep_mask.as_ref().and_then(|m| m[source_idx].as_deref()) {
            if keep_mask.len() != source_rows {
                return Err(Error::ArrowSerialize(format!(
                    "Dynamic keep-mask mismatch for '{}': {} != {source_rows}",
                    field.logical_type,
                    keep_mask.len()
                )));
            }
            let keep = keep_mask.iter().copied().map(Some).collect::<BooleanArray>();
            values = filter(values.as_ref(), &keep)?;
        }

        field.logical_type.serialize_async(writer, &values, values.data_type(), state).await?;
    }

    Ok(())
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    if !matches!(type_.strip_null(), Type::Dynamic { .. }) {
        return Err(Error::ArrowSerialize(format!("Dynamic serialize type unsupported: {type_}")));
    }

    let Some(union) = column.as_any().downcast_ref::<UnionArray>() else {
        return Err(Error::ArrowSerialize("Expected UnionArray for Dynamic serialization".into()));
    };

    let dynamic_prefix = state.take_dynamic_prefix().ok_or_else(|| {
        Error::ArrowSerialize("Dynamic serialization requires prefix metadata".to_string())
    })?;

    let (fields, by_type_id) = parse_dynamic_union_schema(data_type, dynamic_prefix)?;
    let total_types = fields.len();
    let null_discriminator = total_types;
    let discriminator_width = dynamic_index_width(total_types);
    let mut source_counts = vec![0_usize; total_types];
    let mut source_keep_mask: Option<Vec<Option<Vec<bool>>>> = None;

    for row in 0..union.len() {
        let row_type_id = union.type_id(row);
        let source_idx = by_type_id.resolve(row_type_id)?;
        let value_offset = union.value_offset(row);
        let child = union.child(row_type_id);
        if source_counts[source_idx] <= value_offset {
            source_counts[source_idx] = value_offset + 1;
            if let Some(masks) = source_keep_mask.as_mut()
                && let Some(mask) = masks[source_idx].as_mut()
            {
                mask.resize(value_offset + 1, true);
            }
        }

        if child.is_null(value_offset) {
            write_dynamic_index_sync(writer, discriminator_width, null_discriminator)?;
            let masks = source_keep_mask.get_or_insert_with(|| vec![None; total_types]);
            let mask =
                masks[source_idx].get_or_insert_with(|| vec![true; source_counts[source_idx]]);
            if mask.len() <= value_offset {
                mask.resize(value_offset + 1, true);
            }
            mask[value_offset] = false;
            continue;
        }

        write_dynamic_index_sync(writer, discriminator_width, source_idx)?;
        if let Some(masks) = source_keep_mask.as_mut()
            && let Some(mask) = masks[source_idx].as_mut()
        {
            if mask.len() <= value_offset {
                mask.resize(value_offset + 1, true);
            }
            mask[value_offset] = true;
        }
    }

    for (source_idx, field) in fields.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let mut values = union.child(field.type_id).clone();
        if source_rows < values.len() {
            values = values.slice(0, source_rows);
        } else if source_rows > values.len() {
            return Err(Error::ArrowSerialize(format!(
                "Dynamic child '{}' row count mismatch: {} > {}",
                field.logical_type,
                source_rows,
                values.len()
            )));
        }

        if let Some(keep_mask) = source_keep_mask.as_ref().and_then(|m| m[source_idx].as_deref()) {
            if keep_mask.len() != source_rows {
                return Err(Error::ArrowSerialize(format!(
                    "Dynamic keep-mask mismatch for '{}': {} != {source_rows}",
                    field.logical_type,
                    keep_mask.len()
                )));
            }
            let keep = keep_mask.iter().copied().map(Some).collect::<BooleanArray>();
            values = filter(values.as_ref(), &keep)?;
        }

        field.logical_type.serialize(writer, &values, values.data_type(), state)?;
    }

    Ok(())
}
