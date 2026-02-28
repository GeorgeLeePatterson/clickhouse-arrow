use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UnionArray};
use arrow::datatypes::{DataType, UnionMode};
use tokio::io::AsyncWriteExt;

use super::ClickHouseArrowSerializer;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

const NULL_DISCRIMINATOR: u8 = u8::MAX;
const LOOKUP_UNSET: i16 = i16::MIN;
const LOOKUP_NULL: i16 = -1;

#[derive(Clone, Copy)]
struct VariantSchema {
    type_ids: [i8; 256],
    lookup:   [i16; 256],
}

impl VariantSchema {
    #[expect(clippy::cast_sign_loss)]
    fn slot(type_id: i8) -> usize { (i16::from(type_id) + 128) as usize }

    fn parse(type_hint: &Type, data_type: &DataType) -> Result<Self> {
        let Type::Variant(variants) = type_hint.strip_null() else {
            return Err(Error::ArrowSerialize(format!(
                "Variant serializer called with non-Variant type: {type_hint}"
            )));
        };

        let DataType::Union(fields, UnionMode::Dense) = data_type else {
            return Err(Error::ArrowSerialize(format!(
                "Variant serialization expects Arrow DenseUnion, found {data_type:?}"
            )));
        };

        if variants.is_empty() {
            return Err(Error::ArrowSerialize(
                "Variant requires at least one nested type".to_string(),
            ));
        }

        if fields.len() != variants.len() + 1 {
            return Err(Error::ArrowSerialize(format!(
                "Variant union child count mismatch: schema={}, expected={}",
                fields.len(),
                variants.len() + 1
            )));
        }

        let mut lookup = [LOOKUP_UNSET; 256];
        let mut type_ids = [0_i8; 256];

        for (variant_idx, (type_id, _field)) in fields.iter().take(variants.len()).enumerate() {
            let slot = Self::slot(type_id);
            if lookup[slot] != LOOKUP_UNSET {
                return Err(Error::ArrowSerialize(format!(
                    "Variant union has duplicate type id {type_id}"
                )));
            }
            lookup[slot] = i16::try_from(variant_idx).map_err(|_| {
                Error::ArrowSerialize(format!(
                    "Variant index {variant_idx} exceeds supported discriminator range"
                ))
            })?;
            type_ids[variant_idx] = type_id;
        }

        let (null_type_id, _) = fields.iter().nth(variants.len()).ok_or_else(|| {
            Error::ArrowSerialize("Variant union schema is missing null child".to_string())
        })?;
        let null_slot = Self::slot(null_type_id);
        if lookup[null_slot] != LOOKUP_UNSET {
            return Err(Error::ArrowSerialize(format!(
                "Variant null child reuses non-null type id {null_type_id}"
            )));
        }
        lookup[null_slot] = LOOKUP_NULL;

        Ok(Self { type_ids, lookup })
    }

    fn resolve(&self, type_id: i8) -> Result<i16> {
        let value = self.lookup[Self::slot(type_id)];
        if value == LOOKUP_UNSET {
            return Err(Error::ArrowSerialize(format!(
                "Variant union row references unknown type id {type_id}"
            )));
        }
        Ok(value)
    }
}

pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    state: &mut crate::formats::SerializerState,
) -> Result<()> {
    let union = column.as_any().downcast_ref::<UnionArray>().ok_or_else(|| {
        Error::ArrowSerialize("Expected UnionArray for Variant serialization".into())
    })?;
    let Type::Variant(variants) = type_hint.strip_null() else {
        return Err(Error::ArrowSerialize(format!(
            "Variant serializer called with non-Variant type: {type_hint}"
        )));
    };
    let schema = VariantSchema::parse(type_hint, union.data_type())?;

    let mut source_counts = vec![0_usize; variants.len()];

    for row in 0..union.len() {
        let row_type_id = union.type_id(row);
        let resolved = schema.resolve(row_type_id)?;
        if resolved == LOOKUP_NULL {
            writer.write_u8(NULL_DISCRIMINATOR).await?;
            continue;
        }

        #[expect(clippy::cast_sign_loss)]
        let source_idx = resolved as usize;
        let value_offset = union.value_offset(row);
        if source_counts[source_idx] <= value_offset {
            source_counts[source_idx] = value_offset + 1;
        }
        #[expect(clippy::cast_possible_truncation)]
        writer.write_u8(source_idx as u8).await?;
    }

    for (source_idx, source_type) in variants.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let mut values = Arc::clone(union.child(schema.type_ids[source_idx]));
        if source_rows < values.len() {
            values = values.slice(0, source_rows);
        } else if source_rows > values.len() {
            return Err(Error::ArrowSerialize(format!(
                "Variant child '{}' row count mismatch: {} > {}",
                source_type,
                source_rows,
                values.len()
            )));
        }

        Box::pin(source_type.serialize_async(writer, &values, values.data_type(), state)).await?;
    }

    Ok(())
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    state: &mut crate::formats::SerializerState,
) -> Result<()> {
    let union = column.as_any().downcast_ref::<UnionArray>().ok_or_else(|| {
        Error::ArrowSerialize("Expected UnionArray for Variant serialization".into())
    })?;
    let Type::Variant(variants) = type_hint.strip_null() else {
        return Err(Error::ArrowSerialize(format!(
            "Variant serializer called with non-Variant type: {type_hint}"
        )));
    };
    let schema = VariantSchema::parse(type_hint, union.data_type())?;

    let mut source_counts = vec![0_usize; variants.len()];

    for row in 0..union.len() {
        let row_type_id = union.type_id(row);
        let resolved = schema.resolve(row_type_id)?;
        if resolved == LOOKUP_NULL {
            writer.put_u8(NULL_DISCRIMINATOR);
            continue;
        }

        #[expect(clippy::cast_sign_loss)]
        let source_idx = resolved as usize;
        let value_offset = union.value_offset(row);
        if source_counts[source_idx] <= value_offset {
            source_counts[source_idx] = value_offset + 1;
        }
        #[expect(clippy::cast_possible_truncation)]
        writer.put_u8(source_idx as u8);
    }

    for (source_idx, source_type) in variants.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let mut values = Arc::clone(union.child(schema.type_ids[source_idx]));
        if source_rows < values.len() {
            values = values.slice(0, source_rows);
        } else if source_rows > values.len() {
            return Err(Error::ArrowSerialize(format!(
                "Variant child '{}' row count mismatch: {} > {}",
                source_type,
                source_rows,
                values.len()
            )));
        }

        source_type.serialize(writer, &values, values.data_type(), state)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Int32Array, NullArray, StringArray};
    use arrow::datatypes::{Field, UnionFields};

    use super::*;
    use crate::formats::SerializerState;

    fn variant_data_type() -> DataType {
        DataType::Union(
            UnionFields::new([0_i8, 1_i8, 2_i8], vec![
                Field::new("Int32", DataType::Int32, false),
                Field::new("String", DataType::Utf8, false),
                Field::new("Nothing", DataType::Null, false),
            ]),
            UnionMode::Dense,
        )
    }

    fn variant_array() -> ArrayRef {
        let DataType::Union(fields, _) = variant_data_type() else { unreachable!() };
        Arc::new(
            UnionArray::try_new(
                fields,
                vec![0_i8, 1_i8, 2_i8, 0_i8].into(),
                Some(vec![0_i32, 0, 0, 1].into()),
                vec![
                    Arc::new(Int32Array::from(vec![10_i32, 20])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                    Arc::new(NullArray::new(1)) as ArrayRef,
                ],
            )
            .unwrap(),
        ) as ArrayRef
    }

    #[tokio::test]
    async fn test_serialize_variant_dense_union_async() {
        let type_hint = Type::Variant(vec![Type::Int32, Type::String]);
        let column = variant_array();
        let mut writer = Cursor::new(Vec::new());

        serialize_async(&type_hint, &mut writer, &column, &mut SerializerState::default())
            .await
            .unwrap();

        let output = writer.into_inner();
        assert_eq!(output, vec![0_u8, 1, 255, 0, 10, 0, 0, 0, 20, 0, 0, 0, 1, b'a',]);
    }

    #[test]
    fn test_serialize_variant_dense_union_sync() {
        let type_hint = Type::Variant(vec![Type::Int32, Type::String]);
        let column = variant_array();
        let mut writer = Vec::new();

        serialize(&type_hint, &mut writer, &column, &mut SerializerState::default()).unwrap();

        assert_eq!(writer, vec![0_u8, 1, 255, 0, 10, 0, 0, 0, 20, 0, 0, 0, 1, b'a',]);
    }
}
