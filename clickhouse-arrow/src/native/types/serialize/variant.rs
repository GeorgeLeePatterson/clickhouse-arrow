use tokio::io::AsyncWriteExt;

use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct VariantSerializer;

impl Serializer for VariantSerializer {
    fn write_prefix_sync(
        type_: &Type,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) {
        if let Type::Variant(variants) = type_ {
            writer.put_u64_le(0);
            for variant in variants {
                variant.serialize_prefix(writer, state);
            }
        }
    }

    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        if let Type::Variant(variants) = type_ {
            writer.write_u64_le(0).await?;
            for variant in variants {
                variant.serialize_prefix_async(writer, state).await?;
            }
        }
        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::SerializeError(
            "Variant native value serialization is not implemented".to_string(),
        ))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::SerializeError(
            "Variant native value serialization is not implemented".to_string(),
        ))
    }
}
