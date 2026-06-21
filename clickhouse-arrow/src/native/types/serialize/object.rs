use tokio::io::AsyncWriteExt;

use super::{Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct ObjectSerializer;

impl Serializer for ObjectSerializer {
    fn write_prefix_sync(
        _type_: &Type,
        writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) {
        writer.put_i8(1);
    }

    async fn write_prefix<W: ClickHouseWrite>(
        _type_: &Type,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        // Corresponds to STRING serialization in native protocol
        // See: https://github.com/ClickHouse/ClickHouse/blob/6fb23dee26fdee776c014e735436a4e670c99d82/src/DataTypes/Serializations/SerializationObject.cpp#L216
        writer.write_u8(1).await?;
        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        for value in values {
            let value = if value == Value::Null { type_.default_value() } else { value };
            match value {
                Value::Object(bytes) => writer.write_string(bytes).await?,
                _ => {
                    return Err(Error::serialize(format!(
                        "ObjectSerializer unimplemented: {type_:?} for value = {value:?}",
                    )));
                }
            }
        }
        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        for value in values {
            let value = if value == Value::Null { type_.default_value() } else { value };
            match value {
                Value::Object(bytes) => writer.put_string(bytes)?,
                _ => {
                    return Err(Error::serialize(format!(
                        "ObjectSerializer unimplemented: {type_:?} for value = {value:?}",
                    )));
                }
            }
        }
        Ok(())
    }
}
