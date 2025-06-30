use std::net::{Ipv4Addr, Ipv6Addr};

use tokio::io::AsyncReadExt;
use uuid::Uuid;

use super::{Deserializer, DeserializerState, Type};
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::native::values::Value;
use crate::{Date, Date32, DateTime, DynDateTime64, Result, i256, u256};

pub(crate) struct SizedDeserializer;

impl Deserializer for SizedDeserializer {
    async fn read<R: ClickHouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::with_capacity(rows);
        for _ in 0..rows {
            out.push(match type_ {
                Type::Int8 => Value::Int8(reader.read_i8().await?),
                Type::Int16 => Value::Int16(reader.read_i16_le().await?),
                Type::Int32 => Value::Int32(reader.read_i32_le().await?),
                Type::Int64 => Value::Int64(reader.read_i64_le().await?),
                Type::Int128 => Value::Int128(reader.read_i128_le().await?),
                Type::Int256 => {
                    let mut buf = [0u8; 32];
                    let _ = reader.read_exact(&mut buf[..]).await?;
                    buf.reverse();
                    Value::Int256(i256(buf))
                }
                Type::UInt8 => Value::UInt8(reader.read_u8().await?),
                Type::UInt16 => Value::UInt16(reader.read_u16_le().await?),
                Type::UInt32 => Value::UInt32(reader.read_u32_le().await?),
                Type::UInt64 => Value::UInt64(reader.read_u64_le().await?),
                Type::UInt128 => Value::UInt128(reader.read_u128_le().await?),
                Type::UInt256 => {
                    let mut buf = [0u8; 32];
                    let _ = reader.read_exact(&mut buf[..]).await?;
                    buf.reverse();
                    Value::UInt256(u256(buf))
                }
                Type::Float32 => Value::Float32(f32::from_bits(reader.read_u32_le().await?)),
                Type::Float64 => Value::Float64(f64::from_bits(reader.read_u64_le().await?)),
                Type::Decimal32(s) => Value::Decimal32(*s, reader.read_i32_le().await?),
                Type::Decimal64(s) => Value::Decimal64(*s, reader.read_i64_le().await?),
                Type::Decimal128(s) => Value::Decimal128(*s, reader.read_i128_le().await?),
                Type::Decimal256(s) => {
                    let mut buf = [0u8; 32];
                    let _ = reader.read_exact(&mut buf[..]).await?;
                    buf.reverse();
                    Value::Decimal256(*s, i256(buf))
                }
                Type::Uuid => Value::Uuid({
                    let n1 = reader.read_u64_le().await?;
                    let n2 = reader.read_u64_le().await?;
                    Uuid::from_u128((u128::from(n1) << 64) | u128::from(n2))
                }),
                Type::Date => Value::Date(Date(reader.read_u16_le().await?)),
                Type::Date32 => Value::Date32(Date32(reader.read_i32_le().await?)),
                Type::DateTime(tz) => Value::DateTime(DateTime(*tz, reader.read_u32_le().await?)),
                Type::Ipv4 => Value::Ipv4(Ipv4Addr::from(reader.read_u32_le().await?).into()),
                Type::Ipv6 => {
                    let mut octets = [0u8; 16];
                    let _ = reader.read_exact(&mut octets[..]).await?;
                    Value::Ipv6(Ipv6Addr::from(octets).into())
                }
                Type::DateTime64(precision, tz) => {
                    let raw = reader.read_u64_le().await?;
                    Value::DateTime64(DynDateTime64(*tz, raw, *precision))
                }
                Type::Enum8(pairs) => {
                    let idx = reader.read_i8().await?;
                    let value = pairs.iter().find(|(_, i)| *i == idx).ok_or(
                        crate::Error::DeserializeError(format!("Invalid enum8 index: {idx}")),
                    )?;
                    Value::Enum8(value.0.clone(), idx)
                }
                Type::Enum16(pairs) => {
                    let idx = reader.read_i16_le().await?;
                    let value = pairs.iter().find(|(_, i)| *i == idx).ok_or(
                        crate::Error::DeserializeError(format!("Invalid enum8 index: {idx}")),
                    )?;
                    Value::Enum16(value.0.clone(), idx)
                }
                _ => {
                    return Err(crate::Error::DeserializeError(format!(
                        "SizedDeserializer unimplemented: {type_:?}"
                    )));
                }
            });
        }
        Ok(out)
    }

    fn read_sync(
        type_: &Type,
        reader: &mut impl ClickHouseBytesRead,
        rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::with_capacity(rows);
        for _ in 0..rows {
            out.push(match type_ {
                Type::Int8 => Value::Int8(reader.try_get_i8()?),
                Type::Int16 => Value::Int16(reader.try_get_i16_le()?),
                Type::Int32 => Value::Int32(reader.try_get_i32_le()?),
                Type::Int64 => Value::Int64(reader.try_get_i64_le()?),
                Type::Int128 => Value::Int128(reader.try_get_i128_le()?),
                Type::Int256 => {
                    let mut buf = [0u8; 32];
                    reader.try_copy_to_slice(&mut buf[..])?;
                    buf.reverse();
                    Value::Int256(i256(buf))
                }
                Type::UInt8 => Value::UInt8(reader.try_get_u8()?),
                Type::UInt16 => Value::UInt16(reader.try_get_u16_le()?),
                Type::UInt32 => Value::UInt32(reader.try_get_u32_le()?),
                Type::UInt64 => Value::UInt64(reader.try_get_u64_le()?),
                Type::UInt128 => Value::UInt128(reader.try_get_u128_le()?),
                Type::UInt256 => {
                    let mut buf = [0u8; 32];
                    reader.try_copy_to_slice(&mut buf[..])?;
                    buf.reverse();
                    Value::UInt256(u256(buf))
                }
                Type::Float32 => Value::Float32(f32::from_bits(reader.try_get_u32_le()?)),
                Type::Float64 => Value::Float64(f64::from_bits(reader.try_get_u64_le()?)),
                Type::Decimal32(s) => Value::Decimal32(*s, reader.try_get_i32_le()?),
                Type::Decimal64(s) => Value::Decimal64(*s, reader.try_get_i64_le()?),
                Type::Decimal128(s) => Value::Decimal128(*s, reader.try_get_i128_le()?),
                Type::Decimal256(s) => {
                    let mut buf = [0u8; 32];
                    reader.try_copy_to_slice(&mut buf[..])?;
                    buf.reverse();
                    Value::Decimal256(*s, i256(buf))
                }
                Type::Uuid => Value::Uuid({
                    let n1 = reader.try_get_u64_le()?;
                    let n2 = reader.try_get_u64_le()?;
                    Uuid::from_u128((u128::from(n1) << 64) | u128::from(n2))
                }),
                Type::Date => Value::Date(Date(reader.try_get_u16_le()?)),
                Type::Date32 => Value::Date32(Date32(reader.try_get_i32_le()?)),
                Type::DateTime(tz) => Value::DateTime(DateTime(*tz, reader.try_get_u32_le()?)),
                Type::Ipv4 => Value::Ipv4(Ipv4Addr::from(reader.try_get_u32_le()?).into()),
                Type::Ipv6 => {
                    let mut octets = [0u8; 16];
                    reader.try_copy_to_slice(&mut octets[..])?;
                    Value::Ipv6(Ipv6Addr::from(octets).into())
                }
                Type::DateTime64(precision, tz) => {
                    let raw = reader.try_get_u64_le()?;
                    Value::DateTime64(DynDateTime64(*tz, raw, *precision))
                }
                Type::Enum8(pairs) => {
                    let idx = reader.try_get_i8()?;
                    let value = pairs.iter().find(|(_, i)| *i == idx).ok_or(
                        crate::Error::DeserializeError(format!("Invalid enum8 index: {idx}")),
                    )?;
                    Value::Enum8(value.0.clone(), idx)
                }
                Type::Enum16(pairs) => {
                    let idx = reader.try_get_i16_le()?;
                    let value = pairs.iter().find(|(_, i)| *i == idx).ok_or(
                        crate::Error::DeserializeError(format!("Invalid enum16 index: {idx}")),
                    )?;
                    Value::Enum16(value.0.clone(), idx)
                }
                _ => {
                    return Err(crate::Error::DeserializeError(format!(
                        "SizedDeserializer unimplemented: {type_:?}"
                    )));
                }
            });
        }
        Ok(out)
    }
}
