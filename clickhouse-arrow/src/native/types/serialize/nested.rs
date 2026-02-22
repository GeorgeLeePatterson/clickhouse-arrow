use super::{ClickHouseNativeSerializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Serializer, Value};

pub(crate) struct NestedSerializer;

impl Serializer for NestedSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        if let Type::Nested(fields) = type_ {
            for (_, field_type) in fields {
                field_type.serialize_prefix_async(writer, state).await?;
            }
        }
        Ok(())
    }

    fn write_prefix_sync(
        type_: &Type,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) {
        if let Type::Nested(fields) = type_ {
            for (_, field_type) in fields {
                field_type.serialize_prefix(writer, state);
            }
        }
    }

    async fn write<W: ClickHouseWrite>(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("Nested native value serialization is not implemented"))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("Nested native value serialization is not implemented"))
    }
}
