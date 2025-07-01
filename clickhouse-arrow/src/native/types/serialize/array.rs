use tokio::io::AsyncWriteExt;

use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::prelude::*;
use crate::{Result, Value};

// Trait to allow serializing [Values] wrapping an array of items.
pub(crate) trait ArraySerializerGeneric {
    fn inner_type(type_: &Type) -> Result<&Type>;
    fn value_len(value: &Value) -> Result<usize>;
    fn values(value: Value) -> Result<Vec<Value>>;
}

pub(crate) struct ArraySerializer;
impl ArraySerializerGeneric for ArraySerializer {
    fn value_len(value: &Value) -> Result<usize> { value.unwrap_array_ref().map(<[Value]>::len) }

    fn inner_type(type_: &Type) -> Result<&Type> { type_.unwrap_array() }

    fn values(value: Value) -> Result<Vec<Value>> { value.unwrap_array() }
}

impl<T: ArraySerializerGeneric + 'static> Serializer for T {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        T::inner_type(type_)?.serialize_prefix_async(writer, state).await
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let type_ = T::inner_type(type_)?;

        let mut offset = 0usize;
        for value in &values {
            offset += Self::value_len(value)?;
            writer.write_u64_le(offset as u64).await?;
        }
        let mut all_values: Vec<Value> = Vec::with_capacity(offset);
        for value in values {
            all_values.append(&mut Self::values(value)?);
        }

        match type_.serialize_column(all_values, writer, state).await {
            Ok(()) => {}
            Err(e) => {
                error!("error serializing column in array type={type_:?}: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) -> Result<()> {
        let type_ = T::inner_type(type_)?;

        let mut offset = 0usize;
        for value in &values {
            offset += Self::value_len(value)?;
            writer.put_u64_le(offset as u64);
        }
        let mut all_values: Vec<Value> = Vec::with_capacity(offset);
        for value in values {
            all_values.append(&mut Self::values(value)?);
        }

        match type_.serialize_column_sync(all_values, writer, state) {
            Ok(()) => {}
            Err(e) => {
                error!("error serializing column in array type={type_:?}: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }
}
