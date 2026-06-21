use super::{Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct QBitSerializer;

impl Serializer for QBitSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        if !matches!(type_, Type::QBit { .. }) {
            return Err(Error::serialize("QBitSerializer called with non-qbit type"));
        }
        Ok(())
    }

    fn write_prefix_sync(
        type_: &Type,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) {
        assert!(matches!(type_, Type::QBit { .. }), "QBitSerializer called with non-qbit type");
    }

    async fn write<W: ClickHouseWrite>(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("QBit native value serialization is not implemented"))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("QBit native value serialization is not implemented"))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn qbit_type() -> Type { Type::QBit { element_type: Box::new(Type::Float32), dimension: 3 } }

    #[tokio::test]
    async fn write_prefix_async_accepts_qbit_type() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        QBitSerializer::write_prefix(&qbit_type(), &mut writer, &mut state).await.unwrap();
        assert!(writer.into_inner().is_empty());
    }

    #[tokio::test]
    async fn write_prefix_async_rejects_non_qbit_type() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        let error =
            QBitSerializer::write_prefix(&Type::UInt8, &mut writer, &mut state).await.unwrap_err();
        assert!(error.to_string().contains("non-qbit"));
    }

    #[test]
    fn write_prefix_sync_accepts_qbit_type() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        QBitSerializer::write_prefix_sync(&qbit_type(), &mut writer, &mut state);
        assert!(writer.is_empty());
    }

    #[test]
    #[should_panic(expected = "non-qbit type")]
    fn write_prefix_sync_rejects_non_qbit_type() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        QBitSerializer::write_prefix_sync(&Type::UInt8, &mut writer, &mut state);
    }

    #[tokio::test]
    async fn write_async_returns_unimplemented_error() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        let error =
            QBitSerializer::write(&qbit_type(), vec![], &mut writer, &mut state).await.unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }

    #[test]
    fn write_sync_returns_unimplemented_error() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        let error =
            QBitSerializer::write_sync(&qbit_type(), vec![], &mut writer, &mut state).unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }
}
