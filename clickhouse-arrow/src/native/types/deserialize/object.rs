use tokio::io::AsyncReadExt;

use super::{Deserializer, DeserializerState, Type};
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::native::values::Value;
use crate::{Error, Result};

pub(crate) struct ObjectDeserializer;

#[allow(clippy::uninit_vec)]
impl Deserializer for ObjectDeserializer {
    async fn read_prefix<R: ClickHouseRead>(
        type_: &Type,
        reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Object => {
                let _ = reader.read_i8().await?;
            }
            _ => {
                return Err(Error::DeserializeError(
                    "ObjectDeserializer called with non-json type".to_string(),
                ));
            }
        }
        Ok(())
    }

    async fn read<R: ClickHouseRead>(
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
            _ => Err(Error::DeserializeError(
                "ObjectDeserializer called with non-json type".to_string(),
            )),
        }
    }

    fn read_sync(
        type_: &Type,
        reader: &mut impl ClickHouseBytesRead,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        match type_ {
            Type::Object | Type::String | Type::Binary => {
                let mut out = Vec::with_capacity(rows);
                for _ in 0..rows {
                    let value = reader.try_get_string()?;
                    out.push(if matches!(type_, Type::Object) {
                        Value::Object(value.to_vec())
                    } else {
                        Value::String(value.to_vec())
                    });
                }
                Ok(out)
            }
            _ => Err(Error::DeserializeError(
                "ObjectDeserializer called with non-json type".to_string(),
            )),
        }
    }
}
