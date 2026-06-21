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
        Err(Error::serialize("Variant native value serialization is not implemented"))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("Variant native value serialization is not implemented"))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn variant_type() -> Type { Type::Variant(vec![Type::UInt8, Type::String]) }

    #[tokio::test]
    async fn write_prefix_async_is_noop_for_non_variant() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        VariantSerializer::write_prefix(&Type::UInt8, &mut writer, &mut state).await.unwrap();
        assert!(writer.into_inner().is_empty());
    }

    #[tokio::test]
    async fn write_prefix_async_writes_version_word() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        VariantSerializer::write_prefix(&variant_type(), &mut writer, &mut state).await.unwrap();
        assert_eq!(&writer.into_inner()[..8], &0_u64.to_le_bytes());
    }

    #[test]
    fn write_prefix_sync_is_noop_for_non_variant() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        VariantSerializer::write_prefix_sync(&Type::UInt8, &mut writer, &mut state);
        assert!(writer.is_empty());
    }

    #[test]
    fn write_prefix_sync_writes_version_word() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        VariantSerializer::write_prefix_sync(&variant_type(), &mut writer, &mut state);
        assert_eq!(&writer[..8], &0_u64.to_le_bytes());
    }

    #[tokio::test]
    async fn write_async_returns_unimplemented_error() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        let error = VariantSerializer::write(&variant_type(), vec![], &mut writer, &mut state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }

    #[test]
    fn write_sync_returns_unimplemented_error() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        let error = VariantSerializer::write_sync(&variant_type(), vec![], &mut writer, &mut state)
            .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }
}
