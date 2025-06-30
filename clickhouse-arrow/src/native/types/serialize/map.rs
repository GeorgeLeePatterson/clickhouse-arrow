use tokio::io::AsyncWriteExt;

use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct MapSerializer;

impl Serializer for MapSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Map(key, value) => {
                let nested =
                    Type::Array(Box::new(Type::Tuple(vec![(**key).clone(), (**value).clone()])));
                nested.serialize_prefix_async(writer, state).await?;
            }
            _ => {
                return Err(Error::SerializeError(format!(
                    "MapSerializer called with non-map type: {type_:?}"
                )));
            }
        }
        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let Type::Map(key_type, value_type) = type_ else {
            return Err(Error::SerializeError(format!(
                "MapSerializer called with non-map type: {type_:?}"
            )));
        };

        let mut total_keys = vec![];
        let mut total_values = vec![];

        for value in values {
            let Value::Map(keys, values) = value else {
                return Err(Error::SerializeError(format!(
                    "MapSerializer called with non-map value: {value:?}"
                )));
            };
            assert_eq!(keys.len(), values.len());
            writer.write_u64_le((total_keys.len() + keys.len()) as u64).await?;
            total_keys.extend(keys);
            total_values.extend(values);
        }

        key_type.serialize_column(total_keys, writer, state).await?;
        value_type.serialize_column(total_values, writer, state).await?;
        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) -> Result<()> {
        let Type::Map(key_type, value_type) = type_ else {
            return Err(Error::SerializeError(format!(
                "MapSerializer called with non-map type: {type_:?}"
            )));
        };

        let mut total_keys = vec![];
        let mut total_values = vec![];

        for value in values {
            let Value::Map(keys, values) = value else {
                return Err(Error::SerializeError(format!(
                    "MapSerializer called with non-map value: {value:?}"
                )));
            };
            assert_eq!(keys.len(), values.len());
            writer.put_u64_le((total_keys.len() + keys.len()) as u64);
            total_keys.extend(keys);
            total_values.extend(values);
        }

        key_type.serialize_column_sync(total_keys, writer, state)?;
        value_type.serialize_column_sync(total_values, writer, state)?;
        Ok(())
    }
}
