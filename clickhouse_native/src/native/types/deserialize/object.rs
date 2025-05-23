use tokio::io::AsyncReadExt;

use super::{Deserializer, DeserializerState, Type};
use crate::io::ClickhouseRead;
use crate::native::values::Value;
use crate::{ClickhouseNativeError, Result};

pub(crate) struct ObjectDeserializer;

#[allow(clippy::uninit_vec)]
#[async_trait::async_trait]
impl Deserializer for ObjectDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Object => {
                let _ = reader.read_i8().await?;
            }
            _ => {
                return Err(ClickhouseNativeError::DeserializeError(
                    "ObjectDeserializer called with non-json type".to_string(),
                ));
            }
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        match type_ {
            Type::Object | Type::String | Type::Binary => {
                let mut out = Vec::with_capacity(rows);
                for _ in 0..rows {
                    let value = reader.read_string().await?;
                    out.push(if matches!(type_, Type::Object) {
                        Value::Object(value)
                    } else {
                        Value::String(value)
                    });
                }
                Ok(out)
            }
            _ => Err(ClickhouseNativeError::DeserializeError(
                "ObjectDeserializer called with non-json type".to_string(),
            )),
        }
    }
}
