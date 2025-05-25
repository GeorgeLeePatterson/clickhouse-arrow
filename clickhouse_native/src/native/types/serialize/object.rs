use super::{Serializer, SerializerState, Type, ValueSerializer};
use crate::io::ClickhouseWrite;
use crate::{Error, Result, Value};

pub(crate) struct ObjectSerializer;

#[async_trait::async_trait]
impl Serializer for ObjectSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        writer.write_u8(1).await?;
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        for value in values {
            Self::write_value(type_, value, writer, _state).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ValueSerializer for ObjectSerializer {
    async fn write_value<W: ClickhouseWrite>(
        type_: &Type,
        value: Value,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        let value = if value == Value::Null { type_.default_value() } else { value };
        match value {
            Value::Object(bytes) => {
                writer.write_string(bytes).await?;
            }
            _ => {
                return Err(Error::SerializeError(format!(
                    "ObjectSerializer unimplemented: {type_:?} for value = {value:?}",
                )));
            }
        }
        Ok(())
    }
}
