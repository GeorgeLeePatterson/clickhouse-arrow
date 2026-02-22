use tokio::io::AsyncWriteExt;

use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct DynamicSerializer;

impl Serializer for DynamicSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        if let Type::Dynamic { .. } = type_ {
            let prefix = state.take_dynamic_prefix().ok_or_else(|| {
                Error::serialize("Dynamic prefix metadata is required for serialization")
            })?;
            writer.write_u64_le(prefix.serialization_version).await?;
            writer.write_var_uint(prefix.flattened_types.len() as u64).await?;
            for type_ in &prefix.flattened_types {
                writer.write_string(type_.to_string()).await?;
            }

            let total_types = prefix.flattened_types.len();
            let indexes_type = if u8::try_from(total_types).is_ok() {
                Type::UInt8
            } else if u16::try_from(total_types).is_ok() {
                Type::UInt16
            } else if u32::try_from(total_types).is_ok() {
                Type::UInt32
            } else {
                Type::UInt64
            };
            indexes_type.serialize_prefix_async(writer, state).await?;
            for type_ in &prefix.flattened_types {
                type_.serialize_prefix_async(writer, state).await?;
            }
            drop(state.replace_dynamic_prefix(prefix));
        }
        Ok(())
    }

    fn write_prefix_sync(
        type_: &Type,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) {
        if !matches!(type_, Type::Dynamic { .. }) {
            return;
        }

        let Some(prefix) = state.take_dynamic_prefix() else {
            panic!("Dynamic prefix metadata is required for sync serialization");
        };

        writer.put_u64_le(prefix.serialization_version);
        writer
            .put_var_uint(prefix.flattened_types.len() as u64)
            .expect("failed to serialize Dynamic flattened type count");
        for type_ in &prefix.flattened_types {
            writer
                .put_string(type_.to_string())
                .expect("failed to serialize Dynamic flattened type name");
        }

        let total_types = prefix.flattened_types.len();
        let indexes_type = if u8::try_from(total_types).is_ok() {
            Type::UInt8
        } else if u16::try_from(total_types).is_ok() {
            Type::UInt16
        } else if u32::try_from(total_types).is_ok() {
            Type::UInt32
        } else {
            Type::UInt64
        };
        indexes_type.serialize_prefix(writer, state);
        for type_ in &prefix.flattened_types {
            type_.serialize_prefix(writer, state);
        }
        drop(state.replace_dynamic_prefix(prefix));
    }

    async fn write<W: ClickHouseWrite>(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::SerializeError(
            "Dynamic native value serialization is not implemented".to_string(),
        ))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::SerializeError(
            "Dynamic native value serialization is not implemented".to_string(),
        ))
    }
}
