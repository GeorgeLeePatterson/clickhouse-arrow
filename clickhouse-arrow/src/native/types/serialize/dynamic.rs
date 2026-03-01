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
        Err(Error::serialize("Dynamic native value serialization is not implemented"))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("Dynamic native value serialization is not implemented"))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::formats::DynamicPrefixState;

    fn dynamic_prefix() -> DynamicPrefixState {
        DynamicPrefixState {
            serialization_version: 3,
            flattened_types:       vec![Type::UInt8, Type::String],
        }
    }

    #[tokio::test]
    async fn write_prefix_async_is_noop_for_non_dynamic_type() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        DynamicSerializer::write_prefix(&Type::UInt8, &mut writer, &mut state).await.unwrap();
        assert!(writer.into_inner().is_empty());
    }

    #[tokio::test]
    async fn write_prefix_async_requires_dynamic_prefix_metadata() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        let error = DynamicSerializer::write_prefix(
            &Type::Dynamic { max_types: 8 },
            &mut writer,
            &mut state,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("prefix metadata"));
    }

    #[tokio::test]
    async fn write_prefix_async_writes_header_and_restores_state() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix()));

        DynamicSerializer::write_prefix(&Type::Dynamic { max_types: 8 }, &mut writer, &mut state)
            .await
            .unwrap();

        let out = writer.into_inner();
        assert!(!out.is_empty());
        assert_eq!(&out[..8], &3_u64.to_le_bytes());
        assert_eq!(out[8], 2_u8);
        assert!(state.take_dynamic_prefix().is_some());
    }

    #[test]
    fn write_prefix_sync_is_noop_for_non_dynamic_type() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        DynamicSerializer::write_prefix_sync(&Type::UInt8, &mut writer, &mut state);
        assert!(writer.is_empty());
    }

    #[test]
    #[should_panic(expected = "Dynamic prefix metadata is required for sync serialization")]
    fn write_prefix_sync_requires_dynamic_prefix_metadata() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        DynamicSerializer::write_prefix_sync(
            &Type::Dynamic { max_types: 8 },
            &mut writer,
            &mut state,
        );
    }

    #[test]
    fn write_prefix_sync_writes_header_and_restores_state() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix()));

        DynamicSerializer::write_prefix_sync(
            &Type::Dynamic { max_types: 8 },
            &mut writer,
            &mut state,
        );

        assert!(!writer.is_empty());
        assert_eq!(&writer[..8], &3_u64.to_le_bytes());
        assert_eq!(writer[8], 2_u8);
        assert!(state.take_dynamic_prefix().is_some());
    }

    #[tokio::test]
    async fn write_async_returns_unimplemented_error() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        let error = DynamicSerializer::write(
            &Type::Dynamic { max_types: 8 },
            vec![],
            &mut writer,
            &mut state,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }

    #[test]
    fn write_sync_returns_unimplemented_error() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        let error = DynamicSerializer::write_sync(
            &Type::Dynamic { max_types: 8 },
            vec![],
            &mut writer,
            &mut state,
        )
        .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }
}
