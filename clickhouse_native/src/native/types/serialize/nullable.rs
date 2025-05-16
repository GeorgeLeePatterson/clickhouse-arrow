use super::{Serializer, SerializerState, Type};
use crate::io::ClickhouseWrite;
use crate::{ClickhouseNativeError, Result, Value};

pub(crate) struct NullableSerializer;

#[async_trait::async_trait]
impl Serializer for NullableSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = match type_ {
            Type::Nullable(inner) => &**inner,
            _ => {
                return Err(ClickhouseNativeError::SerializeError(
                    "Expected Nullable type".to_string(),
                ));
            }
        };
        // Delegate to inner type's prefix (e.g., LowCardinality)
        inner_type.serialize_prefix(writer, state).await
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let inner_type = if let Type::Nullable(n) = type_ {
            &**n
        } else {
            return Err(ClickhouseNativeError::SerializeError(format!(
                "NullableSerializer called with non-nullable type: {type_:?}"
            )));
        };

        let mask = values.iter().map(|value| u8::from(value == &Value::Null)).collect::<Vec<u8>>();
        writer.write_all(&mask).await?;

        inner_type.serialize_column(values, writer, state).await?;
        Ok(())
    }
}
