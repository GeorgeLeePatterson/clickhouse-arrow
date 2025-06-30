use super::{ClickHouseNativeDeserializer, Deserializer, DeserializerState, Type};
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::native::values::Value;
use crate::{Error, Result};

pub(crate) struct TupleDeserializer;

impl Deserializer for TupleDeserializer {
    async fn read_prefix<R: ClickHouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Tuple(inner) => {
                for item in inner {
                    item.deserialize_prefix_async(reader, state).await?;
                }
            }
            _ => {
                return Err(Error::DeserializeError(
                    "TupleDeserializer called with non-tuple type".to_string(),
                ));
            }
        }
        Ok(())
    }

    async fn read<R: ClickHouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let inner_types = type_.unwrap_tuple()?;
        let mut tuples = vec![Value::Tuple(Vec::with_capacity(inner_types.len())); rows];
        for type_ in inner_types {
            for (i, value) in
                type_.deserialize_column(reader, rows, state).await?.into_iter().enumerate()
            {
                match &mut tuples[i] {
                    Value::Tuple(values) => {
                        values.push(value);
                    }
                    _ => {
                        return Err(Error::DeserializeError("Expected tuple".to_string()));
                    }
                }
            }
        }
        Ok(tuples)
    }

    fn read_sync(
        _type_: &Type,
        _reader: &mut impl ClickHouseBytesRead,
        _rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        Err(Error::DeserializeError("TupleDeserializer sync not yet implemented".to_string()))
    }
}
