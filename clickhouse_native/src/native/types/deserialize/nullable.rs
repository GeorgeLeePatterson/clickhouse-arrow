use tokio::io::AsyncReadExt;

use super::{Deserializer, DeserializerState, Type};
use crate::io::ClickhouseRead;
use crate::native::values::Value;
use crate::{Error, Result};

pub(crate) struct NullableDeserializer;

impl Deserializer for NullableDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        let inner_type = match type_ {
            Type::Nullable(inner) => &**inner,
            _ => {
                return Err(Error::DeserializeError("Expected Nullable type".to_string()));
            }
        };
        // Delegate to inner type
        inner_type.deserialize_prefix(reader, state).await
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        // if mask[i] == 0, item is present
        let mut mask = vec![0u8; rows];
        let _ = reader.read_exact(&mut mask).await?;

        let mut out = type_.strip_null().deserialize_column(reader, rows, state).await?;

        for (i, mask) in mask.iter().enumerate() {
            if *mask != 0 {
                out[i] = Value::Null;
            }
        }

        Ok(out)
    }
}
