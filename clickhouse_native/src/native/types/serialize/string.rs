use super::{Serializer, SerializerState, Type, ValueSerializer};
use crate::io::ClickhouseWrite;
use crate::{Error, Result, Value};

pub(crate) struct StringSerializer;

async fn emit_bytes<W: ClickhouseWrite>(type_: &Type, bytes: &[u8], writer: &mut W) -> Result<()> {
    if let Type::FixedSizedString(s) = type_ {
        if bytes.len() >= *s {
            writer.write_all(&bytes[..*s]).await?;
        } else {
            writer.write_all(bytes).await?;
            let padding = *s - bytes.len();
            for _ in 0..padding {
                writer.write_u8(0).await?;
            }
        }
    } else {
        writer.write_string(bytes).await?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl Serializer for StringSerializer {
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
impl ValueSerializer for StringSerializer {
    async fn write_value<W: ClickhouseWrite>(
        type_: &Type,
        value: Value,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        let value = if value == Value::Null { type_.default_value() } else { value };
        match value {
            Value::String(bytes) => {
                emit_bytes(type_, &bytes, writer).await?;
            }
            Value::Array(items) => {
                // validate function already confirmed the types here (it's an indirect
                // Vec<u8>/Vec<i8>)
                let bytes = items
                    .into_iter()
                    .filter_map(|x| {
                        match x {
                            Value::UInt8(x) => Ok(x),
                            #[expect(clippy::cast_sign_loss)]
                            Value::Int8(x) => Ok(x as u8),
                            // TODO: This is wrong, it will never deserialize w/ missing pieces
                            _ => Err(Error::SerializeError(format!(
                                "StringSerializer called with non-string type: {type_:?}"
                            ))),
                        }
                        .ok()
                    })
                    .collect::<Vec<u8>>();
                emit_bytes(type_, &bytes, writer).await?;
            }
            _ => {
                return Err(Error::SerializeError(format!(
                    "StringSerializer unimplemented: {type_:?} for value = {value:?}",
                )));
            }
        }
        Ok(())
    }
}
