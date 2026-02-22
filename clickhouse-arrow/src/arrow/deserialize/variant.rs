use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UnionArray, new_empty_array, new_null_array};
use arrow::compute::filter;
use arrow::datatypes::{DataType, UnionMode};
use tokio::io::AsyncReadExt;

use crate::arrow::builder::TypedBuilder;
use crate::arrow::deserialize::{ArrowFieldCtx, ClickHouseArrowDeserializer};
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

const NULL_DISCRIMINATOR: u8 = u8::MAX;
const VARIANT_DISCRIMINATOR_MODE_BASIC: u8 = 0;
const VARIANT_DISCRIMINATOR_MODE_COMPACT: u8 = 1;
const VARIANT_COMPACT_GRANULE_FORMAT_PLAIN: u8 = 0;
const VARIANT_COMPACT_GRANULE_FORMAT_COMPACT: u8 = 1;

#[expect(clippy::cast_possible_wrap)]
#[expect(clippy::cast_possible_truncation)]
pub(super) async fn deserialize<R: ClickHouseRead>(
    type_hint: &Type,
    builder: &mut TypedBuilder,
    reader: &mut R,
    data_type: &DataType,
    rows: usize,
    nulls: &[u8],
    ctx: &mut ArrowFieldCtx<'_>,
) -> Result<ArrayRef> {
    let Type::Variant(variants) = type_hint.strip_null() else {
        return Err(Error::ArrowDeserialize(format!(
            "Variant deserializer called with non-Variant type: {type_hint}"
        )));
    };

    let DataType::Union(union_schema_fields, UnionMode::Dense) = data_type else {
        return Err(Error::ArrowDeserialize(format!(
            "Variant deserialization expects Arrow DenseUnion, found {data_type:?}"
        )));
    };

    if variants.is_empty() {
        return Err(Error::ArrowDeserialize(
            "Variant requires at least one nested type".to_string(),
        ));
    }
    if union_schema_fields.len() != variants.len() + 1 {
        return Err(Error::ArrowDeserialize(format!(
            "Variant union child count mismatch: schema={}, expected={}",
            union_schema_fields.len(),
            variants.len() + 1
        )));
    }
    if !nulls.is_empty() && nulls.len() != rows {
        return Err(Error::ArrowDeserialize(format!(
            "Variant outer null mask length mismatch: {} != {rows}",
            nulls.len()
        )));
    }

    let TypedBuilder::Union(union_builder) = builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected builder for Variant: {}",
            builder.as_ref()
        )));
    };
    if union_builder.children.len() != union_schema_fields.len() {
        return Err(Error::ArrowDeserialize(format!(
            "Variant union builder/schema child count mismatch: {} != {}",
            union_builder.children.len(),
            union_schema_fields.len()
        )));
    }

    let null_field_idx = variants.len();
    let discriminator_mode = ctx
        .variant_prefix
        .map_or(VARIANT_DISCRIMINATOR_MODE_BASIC, |state| state.discriminator_mode);
    if discriminator_mode != VARIANT_DISCRIMINATOR_MODE_BASIC
        && discriminator_mode != VARIANT_DISCRIMINATOR_MODE_COMPACT
    {
        return Err(Error::ArrowDeserialize(format!(
            "unsupported Variant discriminator mode {discriminator_mode}; expected 0 (basic) or 1 \
             (compact)"
        )));
    }

    let mut source_counts = vec![0_usize; variants.len()];
    let mut source_keep_mask: Option<Vec<Option<Vec<bool>>>> = None;
    let mut value_offsets = vec![0_usize; variants.len()];
    let mut null_rows = 0_usize;
    let mut type_ids = Vec::with_capacity(rows);
    let mut offsets = Vec::with_capacity(rows);
    let mut compact_granule_format = VARIANT_COMPACT_GRANULE_FORMAT_PLAIN;
    let mut compact_granule_discriminator = 0_u8;
    let mut compact_granule_remaining_rows = 0_usize;

    for outer_null in nulls.iter().copied().chain(std::iter::repeat(0_u8)).take(rows) {
        let discriminator = if discriminator_mode == VARIANT_DISCRIMINATOR_MODE_BASIC {
            reader.read_u8().await?
        } else {
            if compact_granule_remaining_rows == 0 {
                let granule_rows =
                    usize::try_from(reader.read_var_uint().await?).map_err(|_| {
                        Error::ArrowDeserialize(
                            "Variant compact discriminator granule row count exceeds usize"
                                .to_string(),
                        )
                    })?;
                if granule_rows == 0 {
                    return Err(Error::ArrowDeserialize(
                        "Variant compact discriminator granule has zero rows".to_string(),
                    ));
                }

                compact_granule_remaining_rows = granule_rows;
                compact_granule_format = reader.read_u8().await?;
                match compact_granule_format {
                    VARIANT_COMPACT_GRANULE_FORMAT_PLAIN => {}
                    VARIANT_COMPACT_GRANULE_FORMAT_COMPACT => {
                        compact_granule_discriminator = reader.read_u8().await?;
                    }
                    other => {
                        return Err(Error::ArrowDeserialize(format!(
                            "unsupported Variant compact granule format {other}; expected 0 \
                             (plain) or 1 (compact)"
                        )));
                    }
                }
            }

            compact_granule_remaining_rows -= 1;
            if compact_granule_format == VARIANT_COMPACT_GRANULE_FORMAT_COMPACT {
                compact_granule_discriminator
            } else {
                reader.read_u8().await?
            }
        };
        let protocol_null = discriminator == NULL_DISCRIMINATOR;

        let source_idx = if protocol_null {
            None
        } else {
            let source_idx = usize::from(discriminator);
            if source_idx >= variants.len() {
                return Err(Error::ArrowDeserialize(format!(
                    "Variant discriminator {discriminator} is out of bounds for {} nested type(s)",
                    variants.len()
                )));
            }
            Some(source_idx)
        };

        let forced_null = outer_null != 0;
        if let Some(source_idx) = source_idx {
            let source_row_idx = source_counts[source_idx];
            source_counts[source_idx] += 1;

            if forced_null {
                let masks = source_keep_mask.get_or_insert_with(|| vec![None; variants.len()]);
                let keep_mask = masks[source_idx].get_or_insert_with(|| vec![true; source_row_idx]);
                keep_mask.push(false);
            } else if let Some(masks) = source_keep_mask.as_mut()
                && let Some(keep_mask) = masks[source_idx].as_mut()
            {
                keep_mask.push(true);
            }
        }

        if protocol_null || forced_null {
            type_ids.push(union_builder.children[null_field_idx].type_id);
            offsets.push(null_rows as i32);
            null_rows += 1;
        } else if let Some(source_idx) = source_idx {
            type_ids.push(union_builder.children[source_idx].type_id);
            offsets.push(value_offsets[source_idx] as i32);
            value_offsets[source_idx] += 1;
        }
    }

    let mut children = Vec::with_capacity(union_schema_fields.len());
    for (source_idx, source_type) in variants.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let keep_mask = source_keep_mask.as_ref().and_then(|m| m[source_idx].as_deref());
        let source_array = {
            let (child_data_type, child_builder) =
                union_builder.child_parts_mut(source_idx, source_type)?;
            if source_rows == 0 {
                new_empty_array(child_data_type)
            } else {
                let mut child_ctx = ArrowFieldCtx {
                    row_buffer:     ctx.row_buffer,
                    dynamic_prefix: None,
                    variant_prefix: None,
                };
                let source_array = Box::pin(source_type.deserialize_arrow(
                    child_builder,
                    reader,
                    child_data_type,
                    source_rows,
                    &[],
                    &mut child_ctx,
                ))
                .await?;

                if let Some(keep_mask) = keep_mask {
                    if keep_mask.len() != source_rows {
                        return Err(Error::ArrowDeserialize(format!(
                            "Variant keep-mask mismatch for '{}': {} != {source_rows}",
                            source_type,
                            keep_mask.len()
                        )));
                    }
                    let keep = keep_mask.iter().copied().map(Some).collect::<BooleanArray>();
                    filter(source_array.as_ref(), &keep)?
                } else {
                    source_array
                }
            }
        };

        if source_array.len() != value_offsets[source_idx] {
            return Err(Error::ArrowDeserialize(format!(
                "Variant child '{}' row count mismatch: {} != {}",
                source_type,
                source_array.len(),
                value_offsets[source_idx]
            )));
        }

        children.push(source_array);
    }

    if discriminator_mode == VARIANT_DISCRIMINATOR_MODE_COMPACT
        && compact_granule_remaining_rows != 0
    {
        return Err(Error::ArrowDeserialize(format!(
            "Variant compact discriminator granule declared more rows than available in column: \
             {} trailing row(s)",
            compact_granule_remaining_rows
        )));
    }

    let null_data_type = union_builder.data_type(null_field_idx)?;
    children.push(new_null_array(null_data_type, null_rows));

    Ok(Arc::new(UnionArray::try_new(
        union_schema_fields.clone(),
        type_ids.into(),
        Some(offsets.into()),
        children,
    )?))
}
