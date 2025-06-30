use tokio::io::AsyncWriteExt;

use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct NullableSerializer;

impl Serializer for NullableSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = match type_ {
            Type::Nullable(inner) => &**inner,
            _ => {
                return Err(Error::SerializeError("Expected Nullable type".to_string()));
            }
        };

        // Delegate to inner type's prefix (e.g., LowCardinality)
        inner_type.serialize_prefix_async(writer, state).await
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = if let Type::Nullable(n) = type_ {
            &**n
        } else {
            return Err(Error::SerializeError(format!(
                "NullableSerializer called with non-nullable type: {type_:?}"
            )));
        };

        let mask = values.iter().map(|value| u8::from(value == &Value::Null)).collect::<Vec<u8>>();
        writer.write_all(&mask).await?;

        inner_type.serialize_column(values, writer, state).await?;
        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = if let Type::Nullable(n) = type_ {
            &**n
        } else {
            return Err(Error::SerializeError(format!(
                "NullableSerializer called with non-nullable type: {type_:?}"
            )));
        };

        let mask = values.iter().map(|value| u8::from(value == &Value::Null)).collect::<Vec<u8>>();
        writer.put_slice(&mask);

        inner_type.serialize_column_sync(values, writer, state)?;
        Ok(())
    }
}
