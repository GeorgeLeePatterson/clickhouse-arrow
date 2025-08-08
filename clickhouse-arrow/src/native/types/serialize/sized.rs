use tokio::io::AsyncWriteExt;

use super::{Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct SizedSerializer;

fn swap_endian_256(mut input: [u8; 32]) -> [u8; 32] {
    input.reverse();
    input
}

impl Serializer for SizedSerializer {
    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        for value in values {
            match value.justify_null_ref(type_).as_ref() {
                Value::Int8(x) | Value::Enum8(_, x) => writer.write_i8(*x).await?,
                Value::Int16(x) | Value::Enum16(_, x) => writer.write_i16_le(*x).await?,
                Value::Int32(x) | Value::Decimal32(_, x) => writer.write_i32_le(*x).await?,
                Value::Int64(x) | Value::Decimal64(_, x) => writer.write_i64_le(*x).await?,
                Value::Int128(x) | Value::Decimal128(_, x) => writer.write_i128_le(*x).await?,
                Value::Int256(x) | Value::Decimal256(_, x) => {
                    writer.write_all(&swap_endian_256(x.0)[..]).await?;
                }
                Value::UInt8(x) => writer.write_u8(*x).await?,
                Value::UInt16(x) => writer.write_u16_le(*x).await?,
                Value::UInt32(x) => writer.write_u32_le(*x).await?,
                Value::UInt64(x) => writer.write_u64_le(*x).await?,
                Value::UInt128(x) => writer.write_u128_le(*x).await?,
                Value::UInt256(x) => writer.write_all(&swap_endian_256(x.0)[..]).await?,
                Value::Float32(x) => writer.write_u32_le(x.to_bits()).await?,
                Value::Float64(x) => writer.write_u64_le(x.to_bits()).await?,
                Value::Uuid(x) => {
                    let n = x.as_u128();
                    let n1 = (n >> 64) as u64;
                    #[expect(clippy::cast_possible_truncation)]
                    let n2 = n as u64;
                    writer.write_u64_le(n1).await?;
                    writer.write_u64_le(n2).await?;
                }
                Value::Date(x) => writer.write_u16_le(x.0).await?,
                Value::Date32(x) => writer.write_i32_le(x.0).await?,
                Value::DateTime(x) => writer.write_u32_le(x.1).await?,
                Value::DateTime64(x) => writer.write_u64_le(x.1).await?,
                Value::Ipv4(x) => writer.write_u32_le(x.0.into()).await?,
                Value::Ipv6(x) => writer.write_all(&x.octets()[..]).await?,
                _ => {
                    return Err(Error::SerializeError(format!(
                        "SizedSerializer unimplemented: {type_:?} for value = {value:?}",
                    )));
                }
            }
        }
        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        for value in values {
            match value.justify_null_ref(type_).as_ref() {
                Value::Int8(x) | Value::Enum8(_, x) => writer.put_i8(*x),
                Value::Int16(x) | Value::Enum16(_, x) => writer.put_i16_le(*x),
                Value::Int64(x) | Value::Decimal64(_, x) => writer.put_i64_le(*x),
                Value::Int128(x) | Value::Decimal128(_, x) => writer.put_i128_le(*x),
                Value::Int256(x) | Value::Decimal256(_, x) => {
                    writer.put_slice(&swap_endian_256(x.0)[..]);
                }
                Value::UInt8(x) => writer.put_u8(*x),
                Value::UInt16(x) => writer.put_u16_le(*x),
                Value::UInt32(x) => writer.put_u32_le(*x),
                Value::UInt64(x) => writer.put_u64_le(*x),
                Value::UInt128(x) => writer.put_u128_le(*x),
                Value::UInt256(x) => writer.put_slice(&swap_endian_256(x.0)[..]),
                Value::Float32(x) => writer.put_u32_le(x.to_bits()),
                Value::Float64(x) => writer.put_u64_le(x.to_bits()),
                Value::Decimal32(_, x) | Value::Int32(x) => {
                    writer.put_i32_le(*x);
                }
                Value::Uuid(x) => {
                    let n = x.as_u128();
                    let n1 = (n >> 64) as u64;
                    #[expect(clippy::cast_possible_truncation)]
                    let n2 = n as u64;
                    writer.put_u64_le(n1);
                    writer.put_u64_le(n2);
                }
                Value::Date(x) => writer.put_u16_le(x.0),
                Value::Date32(x) => writer.put_i32_le(x.0),
                Value::DateTime(x) => writer.put_u32_le(x.1),
                Value::DateTime64(x) => writer.put_u64_le(x.1),
                Value::Ipv4(x) => writer.put_u32_le(x.0.into()),
                Value::Ipv6(x) => writer.put_slice(&x.octets()[..]),
                _ => {
                    return Err(Error::SerializeError(format!(
                        "SizedSerializer unimplemented: {type_:?} for value = {value:?}",
                    )));
                }
            }
        }
        Ok(())
    }
}
