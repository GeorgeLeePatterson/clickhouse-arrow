use tokio::io::AsyncReadExt;

use super::{Deserializer, DeserializerState, Type};
use crate::Result;
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::native::values::Value;

pub(crate) struct StringDeserializer;

impl Deserializer for StringDeserializer {
    async fn read<R: ClickHouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        match type_ {
            Type::String | Type::Binary => {
                let mut out = Vec::with_capacity(rows);
                for _ in 0..rows {
                    out.push(Value::String(reader.read_string().await?));
                }
                Ok(out)
            }
            Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => {
                let mut out = Vec::with_capacity(rows);
                #[expect(clippy::uninit_vec)]
                for _ in 0..rows {
                    let mut buf = Vec::with_capacity(*n);
                    unsafe { buf.set_len(*n) };
                    let _ = reader.read_exact(&mut buf[..]).await?;
                    let first_null = buf.iter().position(|x| *x == 0).unwrap_or(buf.len());
                    buf.truncate(first_null);
                    out.push(Value::String(buf));
                }
                Ok(out)
            }
            _ => Err(crate::Error::DeserializeError(
                "StringDeserializer called with non-string type".to_string(),
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
            Type::String | Type::Binary => {
                let mut out = Vec::with_capacity(rows);
                for _ in 0..rows {
                    out.push(Value::String(reader.try_get_string()?.to_vec()));
                }
                Ok(out)
            }
            Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => {
                let mut out = Vec::with_capacity(rows);
                for _ in 0..rows {
                    let mut buf = vec![0u8; *n];
                    reader.try_copy_to_slice(&mut buf[..])?;
                    let first_null = buf.iter().position(|x| *x == 0).unwrap_or(buf.len());
                    buf.truncate(first_null);
                    out.push(Value::String(buf));
                }
                Ok(out)
            }
            _ => Err(crate::Error::DeserializeError(
                "StringDeserializer called with non-string type".to_string(),
            )),
        }
    }
}
