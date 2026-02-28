use super::{Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct QBitSerializer;

impl Serializer for QBitSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        if !matches!(type_, Type::QBit { .. }) {
            return Err(Error::serialize("QBitSerializer called with non-qbit type"));
        }
        Ok(())
    }

    fn write_prefix_sync(
        type_: &Type,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) {
        assert!(matches!(type_, Type::QBit { .. }), "QBitSerializer called with non-qbit type");
    }

    async fn write<W: ClickHouseWrite>(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("QBit native value serialization is not implemented"))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("QBit native value serialization is not implemented"))
    }
}
