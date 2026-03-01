use super::{ClickHouseNativeSerializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Serializer, Value};

pub(crate) struct NestedSerializer;

impl Serializer for NestedSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        if let Type::Nested(fields) = type_ {
            for (_, field_type) in fields {
                field_type.serialize_prefix_async(writer, state).await?;
            }
        }
        Ok(())
    }

    fn write_prefix_sync(
        type_: &Type,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) {
        if let Type::Nested(fields) = type_ {
            for (_, field_type) in fields {
                field_type.serialize_prefix(writer, state);
            }
        }
    }

    async fn write<W: ClickHouseWrite>(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("Nested native value serialization is not implemented"))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("Nested native value serialization is not implemented"))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn nested_type() -> Type {
        Type::Nested(vec![("k".to_string(), Type::UInt8), ("v".to_string(), Type::String)])
    }

    #[tokio::test]
    async fn write_prefix_async_is_noop_for_non_nested() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        NestedSerializer::write_prefix(&Type::UInt8, &mut writer, &mut state).await.unwrap();
        assert!(writer.into_inner().is_empty());
    }

    #[tokio::test]
    async fn write_prefix_async_serializes_nested_children() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        NestedSerializer::write_prefix(&nested_type(), &mut writer, &mut state).await.unwrap();
        assert!(writer.into_inner().is_empty());
    }

    #[test]
    fn write_prefix_sync_is_noop_for_non_nested() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        NestedSerializer::write_prefix_sync(&Type::UInt8, &mut writer, &mut state);
        assert!(writer.is_empty());
    }

    #[test]
    fn write_prefix_sync_serializes_nested_children() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        NestedSerializer::write_prefix_sync(&nested_type(), &mut writer, &mut state);
        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn write_async_returns_unimplemented_error() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        let error = NestedSerializer::write(&nested_type(), vec![], &mut writer, &mut state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }

    #[test]
    fn write_sync_returns_unimplemented_error() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        let error = NestedSerializer::write_sync(&nested_type(), vec![], &mut writer, &mut state)
            .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }
}
