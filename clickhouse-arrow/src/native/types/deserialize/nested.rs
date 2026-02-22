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
            _ => Err(Error::DeserializeError(
                "NestedDeserializer called with non-nested type".to_string(),
            )),
        }
    }

    async fn read<R: ClickHouseRead>(
        _type_: &Type,
        _reader: &mut R,
        _rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        Err(Error::DeserializeError(
            "NestedDeserializer native value read is not implemented".to_string(),
        ))
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
