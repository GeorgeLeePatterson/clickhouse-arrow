use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct TupleSerializer;

impl Serializer for TupleSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Tuple(inner) => {
                for item in inner {
                    item.serialize_prefix_async(writer, state).await?;
                }
            }
            _ => {
                return Err(Error::SerializeError(format!(
                    "TupleSerializer called with non-tuple type: {type_:?}"
                )));
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
            return Err(Error::SerializeError(
                "TupleSerializer called with non-tuple type".to_string(),
            ));
        };

        let mut columns = vec![Vec::with_capacity(values.len()); inner_types.len()];

        for value in values {
            let tuple = value.unwrap_tuple()?;
            for (i, value) in tuple.into_iter().enumerate() {
                columns[i].push(value);
            }
        }
        for (inner_type, column) in inner_types.iter().zip(columns.into_iter()) {
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
            return Err(Error::SerializeError(
                "TupleSerializer called with non-tuple type".to_string(),
            ));
        };

        let mut columns = vec![Vec::with_capacity(values.len()); inner_types.len()];

        for value in values {
            let tuple = value.unwrap_tuple()?;
            for (i, value) in tuple.into_iter().enumerate() {
                columns[i].push(value);
            }
        }
        for (inner_type, column) in inner_types.iter().zip(columns.into_iter()) {
            inner_type.serialize_column_sync(column, writer, state)?;
        }
        Ok(())
    }
}
