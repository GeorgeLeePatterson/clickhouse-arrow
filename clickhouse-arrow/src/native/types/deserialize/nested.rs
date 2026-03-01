use super::{ClickHouseNativeDeserializer, Deserializer, DeserializerState, Type};
use crate::io::ClickHouseRead;
use crate::native::values::Value;
use crate::{Error, Result};

pub(crate) struct NestedDeserializer;

impl Deserializer for NestedDeserializer {
    async fn read_prefix<R: ClickHouseRead, T: Default + Send>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState<T>,
    ) -> Result<()> {
        match type_ {
            Type::Nested(fields) => {
                for (_, field_type) in fields {
                    field_type.deserialize_prefix(reader, state).await?;
                }
                Ok(())
            }
            _ => Err(Error::deserialize("NestedDeserializer called with non-nested type")),
        }
    }

    async fn read<R: ClickHouseRead>(
        _type_: &Type,
        _reader: &mut R,
        _rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        Err(Error::deserialize("NestedDeserializer native value read is not implemented"))
    }

    // TODO: Remove
    // fn read_sync(
    //     _type_: &Type,
    //     _reader: &mut impl ClickHouseBytesRead,
    //     _rows: usize,
    //     _state: &mut DeserializerState,
    // ) -> Result<Vec<Value>> {
    //     Err(Error::DeserializeError(
    //         "NestedDeserializer sync native value read is not implemented".to_string(),
    //     ))
    // }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn nested_type() -> Type {
        Type::Nested(vec![("k".to_string(), Type::UInt8), ("v".to_string(), Type::String)])
    }

    #[tokio::test]
    async fn read_prefix_rejects_non_nested_type() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut state = DeserializerState::<()>::default();
        let error = NestedDeserializer::read_prefix(&Type::UInt8, &mut reader, &mut state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("non-nested"));
    }

    #[tokio::test]
    async fn read_prefix_accepts_nested_type() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut state = DeserializerState::<()>::default();
        NestedDeserializer::read_prefix(&nested_type(), &mut reader, &mut state).await.unwrap();
    }

    #[tokio::test]
    async fn read_returns_unimplemented_error() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut state = DeserializerState::<()>::default();
        let error =
            NestedDeserializer::read(&nested_type(), &mut reader, 0, &mut state).await.unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }
}
