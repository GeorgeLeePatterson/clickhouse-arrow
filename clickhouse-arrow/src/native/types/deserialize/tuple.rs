use super::{ClickHouseNativeDeserializer, Deserializer, DeserializerState, Type};
use crate::io::ClickHouseRead;
use crate::native::values::Value;
use crate::{Error, Result};

pub(crate) struct TupleDeserializer;

impl Deserializer for TupleDeserializer {
    async fn read_prefix<R: ClickHouseRead, T: Default + Send>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState<T>,
    ) -> Result<()> {
        match type_ {
            Type::Tuple(inner) => {
                for (idx, (_, item)) in inner.iter().enumerate() {
                    let child_node = state.custom_child_node(idx).or(state.custom_node());
                    let previous_node = state.set_custom_node(child_node);
                    let result = item.deserialize_prefix(reader, state).await;
                    let _ = state.set_custom_node(previous_node);
                    result?;
                }
            }
            _ => {
                return Err(Error::Deserialize(
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
        for (_, type_) in inner_types {
            for (i, value) in
                type_.deserialize_column(reader, rows, state).await?.into_iter().enumerate()
            {
                match &mut tuples[i] {
                    Value::Tuple(values) => {
                        values.push(value);
                    }
                    _ => {
                        return Err(Error::Deserialize("Expected tuple".to_string()));
                    }
                }
            }
        }
        Ok(tuples)
    }

    // TODO: Remove
    // fn read_sync(
    //     _type_: &Type,
    //     _reader: &mut impl ClickHouseBytesRead,
    //     _rows: usize,
    //     _state: &mut DeserializerState,
    // ) -> Result<Vec<Value>> {
    //     Err(Error::DeserializeError("TupleDeserializer sync not yet implemented".to_string()))
    // }
}
