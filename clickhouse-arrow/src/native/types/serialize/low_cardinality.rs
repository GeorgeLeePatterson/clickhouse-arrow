use indexmap::IndexSet;
use tokio::io::AsyncWriteExt;

use super::{Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::native::types::low_cardinality::*;
use crate::{Result, Value};

pub(crate) struct LowCardinalitySerializer;

impl Serializer for LowCardinalitySerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        _type_: &Type,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        writer.write_u64_le(LOW_CARDINALITY_VERSION).await?;
        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = match type_ {
            Type::LowCardinality(x) => &**x,
            _ => {
                return Err(crate::Error::SerializeError(format!(
                    "LowCardinalitySerializer called with non-low-cardinality type: {type_:?}"
                )));
            }
        };

        if values.is_empty() {
            return Ok(());
        }

        let is_nullable = inner_type.is_nullable();
        let inner_type = inner_type.strip_null();

        let mut keys: IndexSet<&Value> = IndexSet::new();
        let nulled = Value::Null;
        if is_nullable {
            let _ = keys.insert(&nulled);
        }
        for value in &values {
            let _ = keys.insert(value);
        }

        let mut flags = 0u64;
        if keys.len() > u32::MAX as usize {
            flags |= TUINT64;
        } else if keys.len() > u16::MAX as usize {
            flags |= TUINT32;
        } else if keys.len() > u8::MAX as usize {
            flags |= TUINT16;
        } else {
            flags |= TUINT8;
        }
        flags |= HAS_ADDITIONAL_KEYS_BIT;
        writer.write_u64_le(flags).await?;

        writer.write_u64_le(keys.len() as u64).await?;

        inner_type
            .serialize_column(keys.iter().copied().cloned().collect(), writer, state)
            .await?;

        writer.write_u64_le(values.len() as u64).await?;
        for value in &values {
            let index = keys.get_index_of(value).unwrap();
            if keys.len() > u32::MAX as usize {
                writer.write_u64_le(index as u64).await?;
            } else if keys.len() > u16::MAX as usize {
                #[expect(clippy::cast_possible_truncation)]
                writer.write_u32_le(index as u32).await?;
            } else if keys.len() > u8::MAX as usize {
                #[expect(clippy::cast_possible_truncation)]
                writer.write_u16_le(index as u16).await?;
            } else {
                #[expect(clippy::cast_possible_truncation)]
                writer.write_u8(index as u8).await?;
            }
        }
        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = match type_ {
            Type::LowCardinality(x) => &**x,
            _ => {
                return Err(crate::Error::SerializeError(format!(
                    "LowCardinalitySerializer called with non-low-cardinality type: {type_:?}"
                )));
            }
        };

        if values.is_empty() {
            return Ok(());
        }

        let is_nullable = inner_type.is_nullable();
        let inner_type = inner_type.strip_null();

        let mut keys: IndexSet<&Value> = IndexSet::new();
        let nulled = Value::Null;
        if is_nullable {
            let _ = keys.insert(&nulled);
        }
        for value in &values {
            let _ = keys.insert(value);
        }

        let mut flags = 0u64;
        if keys.len() > u32::MAX as usize {
            flags |= TUINT64;
        } else if keys.len() > u16::MAX as usize {
            flags |= TUINT32;
        } else if keys.len() > u8::MAX as usize {
            flags |= TUINT16;
        } else {
            flags |= TUINT8;
        }
        flags |= HAS_ADDITIONAL_KEYS_BIT;
        writer.put_u64_le(flags);

        writer.put_u64_le(keys.len() as u64);

        inner_type.serialize_column_sync(keys.iter().copied().cloned().collect(), writer, state)?;

        writer.put_u64_le(values.len() as u64);
        for value in &values {
            let index = keys.get_index_of(value).unwrap();
            if keys.len() > u32::MAX as usize {
                writer.put_u64_le(index as u64);
            } else if keys.len() > u16::MAX as usize {
                #[expect(clippy::cast_possible_truncation)]
                writer.put_u32_le(index as u32);
            } else if keys.len() > u8::MAX as usize {
                #[expect(clippy::cast_possible_truncation)]
                writer.put_u16_le(index as u16);
            } else {
                #[expect(clippy::cast_possible_truncation)]
                writer.put_u8(index as u8);
            }
        }
        Ok(())
    }
}
