use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct TupleSerializer;

impl Serializer for TupleSerializer {
    fn write_prefix_sync(
        type_: &Type,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) {
        if let Type::Tuple(inner) = type_ {
            for (_, item) in inner {
                item.serialize_prefix(writer, state);
            }
        }
    }

    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        if let Type::Tuple(inner) = type_ {
            for (_, item) in inner {
                item.serialize_prefix_async(writer, state).await?;
            }
        }
        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let Type::Tuple(inner_types) = &type_ else {
            return Err(Error::serialize("TupleSerializer called with non-tuple type"));
        };

        let mut columns = vec![Vec::with_capacity(values.len()); inner_types.len()];

        for value in values {
            let tuple = value.unwrap_tuple()?;
            for (i, value) in tuple.into_iter().enumerate() {
                columns[i].push(value);
            }
        }
        for ((_, inner_type), column) in inner_types.iter().zip(columns) {
            inner_type.serialize_column(column, writer, state).await?;
        }
        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) -> Result<()> {
        let Type::Tuple(inner_types) = &type_ else {
            return Err(Error::serialize("TupleSerializer called with non-tuple type"));
        };

        let mut columns = vec![Vec::with_capacity(values.len()); inner_types.len()];

        for value in values {
            let tuple = value.unwrap_tuple()?;
            for (i, value) in tuple.into_iter().enumerate() {
                columns[i].push(value);
            }
        }
        for ((_, inner_type), column) in inner_types.iter().zip(columns) {
            inner_type.serialize_column_sync(column, writer, state)?;
        }
        Ok(())
    }
}
