#[cfg(feature = "extended-types")]
pub(crate) mod aggregate_function;
pub(crate) mod array;
#[cfg(feature = "extended-types")]
pub(crate) mod dynamic;
pub(crate) mod geo;
pub(crate) mod low_cardinality;
pub(crate) mod map;
#[cfg(feature = "extended-types")]
pub(crate) mod nested;
pub(crate) mod nullable;
pub(crate) mod object;
#[cfg(feature = "extended-types")]
pub(crate) mod qbit;
pub(crate) mod sized;
pub(crate) mod string;
pub(crate) mod tuple;
#[cfg(feature = "extended-types")]
pub(crate) mod variant;

use super::*;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};

const MAX_DYNAMIC_TYPE_DEPTH: usize = 1024;

const FIELD_BINARY_NULL: u8 = 0x00;
const FIELD_BINARY_UINT64: u8 = 0x01;
const FIELD_BINARY_INT64: u8 = 0x02;
const FIELD_BINARY_FLOAT64: u8 = 0x07;
const FIELD_BINARY_STRING: u8 = 0x0C;
const FIELD_BINARY_BOOL: u8 = 0x13;
const FIELD_BINARY_NEGATIVE_INFINITY: u8 = 0xFE;
const FIELD_BINARY_POSITIVE_INFINITY: u8 = 0xFF;

impl AggregateParameter {
    fn serialize_dynamic_prefix<W: ClickHouseBytesWrite>(&self, writer: &mut W) -> Result<()> {
        match self {
            Self::Null => writer.put_u8(FIELD_BINARY_NULL),
            Self::Bool(value) => writer.put_slice(&[FIELD_BINARY_BOOL, u8::from(*value)]),
            Self::UInt64(value) => {
                writer.put_u8(FIELD_BINARY_UINT64);
                writer.put_var_uint(*value)?;
            }
            Self::Int64(value) => {
                writer.put_u8(FIELD_BINARY_INT64);
                let encoded = ((*value).cast_unsigned() << 1) ^ (value >> 63).cast_unsigned();
                writer.put_var_uint(encoded)?;
            }
            Self::Float64(bits) => {
                let value = f64::from_bits(*bits);
                if value.is_infinite() {
                    writer.put_u8(if value.is_sign_positive() {
                        FIELD_BINARY_POSITIVE_INFINITY
                    } else {
                        FIELD_BINARY_NEGATIVE_INFINITY
                    });
                } else {
                    writer.put_u8(FIELD_BINARY_FLOAT64);
                    writer.put_slice(&value.to_le_bytes());
                }
            }
            Self::String(value) => {
                writer.put_u8(FIELD_BINARY_STRING);
                writer.put_string(value)?;
            }
        }
        Ok(())
    }
}

pub(crate) trait ClickHouseNativeSerializer {
    fn serialize_dynamic_type<W: ClickHouseBytesWrite>(
        &self,
        writer: &mut W,
        depth: usize,
    ) -> Result<()>;

    fn serialize_prefix_async<'a, W: ClickHouseWrite>(
        &'a self,
        writer: &'a mut W,
        state: &'a mut SerializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    fn serialize_prefix<W: ClickHouseBytesWrite>(
        &self,
        writer: &mut W,
        _state: &mut SerializerState,
    );
}

impl ClickHouseNativeSerializer for Type {
    #[expect(clippy::too_many_lines)]
    fn serialize_dynamic_type<W: ClickHouseBytesWrite>(
        &self,
        writer: &mut W,
        depth: usize,
    ) -> Result<()> {
        fn dynamic_type_tag(type_: &Type) -> u8 {
            match type_.strip_null() {
                Type::Nothing => 0x00,
                Type::UInt8 => 0x01,
                Type::UInt16 => 0x02,
                Type::UInt32 => 0x03,
                Type::UInt64 => 0x04,
                Type::UInt128 => 0x05,
                Type::UInt256 => 0x06,
                Type::Int8 => 0x07,
                Type::Int16 => 0x08,
                Type::Int32 => 0x09,
                Type::Int64 => 0x0A,
                Type::Int128 => 0x0B,
                Type::Int256 => 0x0C,
                Type::Float32 => 0x0D,
                Type::Float64 => 0x0E,
                Type::Date => 0x0F,
                Type::Date32 => 0x10,
                Type::DateTime(tz) if *tz == Tz::UTC => 0x11,
                Type::DateTime(_) => 0x12,
                Type::DateTime64(_, tz) if *tz == Tz::UTC => 0x13,
                Type::DateTime64(_, _) => 0x14,
                Type::String | Type::Binary => 0x15,
                Type::FixedSizedString(_) | Type::FixedSizedBinary(_) => 0x16,
                Type::Enum8(_) => 0x17,
                Type::Enum16(_) => 0x18,
                Type::Decimal32(_) => 0x19,
                Type::Decimal64(_) => 0x1A,
                Type::Decimal128(_) => 0x1B,
                Type::Decimal256(_) => 0x1C,
                Type::Uuid => 0x1D,
                Type::Array(_) => 0x1E,
                Type::Tuple(_) => 0x1F,
                Type::Nullable(_) => 0x23,
                Type::AggregateFunction { .. } => 0x25,
                Type::LowCardinality(_) => 0x26,
                Type::Map(_, _) => 0x27,
                Type::Ipv4 => 0x28,
                Type::Ipv6 => 0x29,
                Type::Variant(_) => 0x2A,
                Type::Dynamic { .. } => 0x2B,
                Type::Point | Type::Ring | Type::Polygon | Type::MultiPolygon => 0x2C,
                Type::SimpleAggregateFunction { .. } => 0x2E,
                Type::Nested(_) => 0x2F,
                Type::Object => 0x30,
                Type::BFloat16 => 0x31,
                Type::Time => 0x32,
                Type::Time64(_) => 0x34,
                Type::QBit { .. } => 0x36,
            }
        }

        if depth > MAX_DYNAMIC_TYPE_DEPTH {
            return Err(Error::SerializeError(format!(
                "type nesting depth exceeds maximum {MAX_DYNAMIC_TYPE_DEPTH}"
            )));
        }

        writer.put_u8(dynamic_type_tag(self));

        match self {
            Type::DateTime(tz) if *tz != Tz::UTC => writer.put_string(tz.name())?,
            Type::DateTime64(scale, tz) => {
                writer.put_u8(*scale);
                if *tz != Tz::UTC {
                    writer.put_string(tz.name())?;
                }
            }
            Type::FixedSizedString(size) | Type::FixedSizedBinary(size) => {
                writer.put_var_uint(*size as u64)?;
            }
            Type::Enum8(values) => {
                writer.put_var_uint(values.len() as u64)?;
                for (name, value) in values {
                    writer.put_string(name)?;
                    writer.put_u8((*value).cast_unsigned());
                }
            }
            Type::Enum16(values) => {
                writer.put_var_uint(values.len() as u64)?;
                for (name, value) in values {
                    writer.put_string(name)?;
                    writer.put_slice(&value.to_le_bytes());
                }
            }
            Type::Decimal32(scale) => writer.put_slice(&[9_u8, *scale]),
            Type::Decimal64(scale) => writer.put_slice(&[18_u8, *scale]),
            Type::Decimal128(scale) => writer.put_slice(&[38_u8, *scale]),
            Type::Decimal256(scale) => writer.put_slice(&[76_u8, *scale]),
            Type::Array(inner) | Type::Nullable(inner) | Type::LowCardinality(inner) => {
                inner.serialize_dynamic_type(writer, depth + 1)?;
            }
            Type::Tuple(t) | Type::Variant(t) => {
                writer.put_var_uint(t.len() as u64)?;
                for type_ in t {
                    type_.serialize_dynamic_type(writer, depth + 1)?;
                }
            }
            Type::Map(key, value) => {
                key.serialize_dynamic_type(writer, depth + 1)?;
                value.serialize_dynamic_type(writer, depth + 1)?;
            }
            Type::Object => {
                writer.put_u8(0); // JSON serialization version
                writer.put_var_uint(0)?; // max_dynamic_paths
                writer.put_u8(0); // max_dynamic_types
                writer.put_var_uint(0)?; // typed_paths
                writer.put_var_uint(0)?; // paths_to_skip
                writer.put_var_uint(0)?; // path_regexps_to_skip
            }
            Type::Nested(fields) => {
                writer.put_var_uint(fields.len() as u64)?;
                for (name, field_type) in fields {
                    writer.put_string(name)?;
                    field_type.serialize_dynamic_type(writer, depth + 1)?;
                }
            }
            Type::Dynamic { max_types } => writer.put_u8(*max_types),
            Type::AggregateFunction { name, parameters, types, version } => {
                writer.put_var_uint(*version)?;
                writer.put_string(name)?;
                writer.put_var_uint(parameters.len() as u64)?;
                for parameter in parameters {
                    parameter.serialize_dynamic_prefix(writer)?;
                }
                writer.put_var_uint(types.len() as u64)?;
                for type_ in types {
                    type_.serialize_dynamic_type(writer, depth + 1)?;
                }
            }
            Type::SimpleAggregateFunction { name, parameters, types } => {
                writer.put_string(name)?;
                writer.put_var_uint(parameters.len() as u64)?;
                for parameter in parameters {
                    parameter.serialize_dynamic_prefix(writer)?;
                }
                writer.put_var_uint(types.len() as u64)?;
                for type_ in types {
                    type_.serialize_dynamic_type(writer, depth + 1)?;
                }
            }
            Type::Point => writer.put_string("Point")?,
            Type::Ring => writer.put_string("Ring")?,
            Type::Polygon => writer.put_string("Polygon")?,
            Type::MultiPolygon => writer.put_string("MultiPolygon")?,
            Type::Time64(scale) => writer.put_u8(*scale),
            Type::QBit { element_type, dimension } => {
                element_type.serialize_dynamic_type(writer, depth + 1)?;
                writer.put_var_uint(*dimension as u64)?;
            }
            Type::Binary
            | Type::Nothing
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::Float32
            | Type::Float64
            | Type::Date
            | Type::Date32
            | Type::DateTime(_)
            | Type::String
            | Type::Uuid
            | Type::Ipv4
            | Type::Ipv6
            | Type::BFloat16
            | Type::Time => {}
        }

        Ok(())
    }

    fn serialize_prefix_async<'a, W: ClickHouseWrite>(
        &'a self,
        writer: &'a mut W,
        state: &'a mut SerializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use serialize::*;
        async move {
            match self {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256
                | Type::Float32
                | Type::Float64
                | Type::Decimal32(_)
                | Type::Decimal64(_)
                | Type::Decimal128(_)
                | Type::Decimal256(_)
                | Type::Uuid
                | Type::Date
                | Type::Date32
                | Type::DateTime(_)
                | Type::DateTime64(_, _)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Enum8(_)
                | Type::Enum16(_) => {
                    sized::SizedSerializer::write_prefix(self, writer, state).await?;
                }
                #[cfg(feature = "extended-types")]
                Type::BFloat16 | Type::Time | Type::Time64(_) => {
                    sized::SizedSerializer::write_prefix(self, writer, state).await?;
                }

                Type::String
                | Type::FixedSizedString(_)
                | Type::Binary
                | Type::FixedSizedBinary(_) => {
                    string::StringSerializer::write_prefix(self, writer, state).await?;
                }

                Type::Array(_) => array::ArraySerializer::write_prefix(self, writer, state).await?,
                Type::Tuple(_) => tuple::TupleSerializer::write_prefix(self, writer, state).await?,
                Type::Point => geo::PointSerializer::write_prefix(self, writer, state).await?,
                Type::Ring => geo::RingSerializer::write_prefix(self, writer, state).await?,
                Type::Polygon => geo::PolygonSerializer::write_prefix(self, writer, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonSerializer::write_prefix(self, writer, state).await?;
                }
                Type::Nullable(_) => {
                    nullable::NullableSerializer::write_prefix(self, writer, state).await?;
                }
                Type::Map(_, _) => map::MapSerializer::write_prefix(self, writer, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalitySerializer::write_prefix(self, writer, state)
                        .await?;
                }
                Type::Object => object::ObjectSerializer::write_prefix(self, writer, state).await?,
                #[cfg(feature = "extended-types")]
                Type::Nested(_) => {
                    nested::NestedSerializer::write_prefix(self, writer, state).await?;
                }
                Type::Nothing => {}
                #[cfg(feature = "extended-types")]
                Type::QBit { .. } => {
                    qbit::QBitSerializer::write_prefix(self, writer, state).await?;
                }
                #[cfg(feature = "extended-types")]
                Type::Variant(_) => {
                    variant::VariantSerializer::write_prefix(self, writer, state).await?;
                }
                #[cfg(feature = "extended-types")]
                Type::Dynamic { .. } => {
                    dynamic::DynamicSerializer::write_prefix(self, writer, state).await?;
                }
                #[cfg(feature = "extended-types")]
                Type::SimpleAggregateFunction { .. } | Type::AggregateFunction { .. } => {
                    aggregate_function::AggregateFunctionSerializer::write_prefix(
                        self, writer, state,
                    )
                    .await?;
                }
            }
            Ok(())
        }
        .boxed()
    }

    fn serialize_prefix<W: ClickHouseBytesWrite>(
        &self,
        writer: &mut W,
        state: &mut SerializerState,
    ) {
        match self {
            Type::Array(_) => array::ArraySerializer::write_prefix_sync(self, writer, state),
            Type::Nullable(_) => {
                nullable::NullableSerializer::write_prefix_sync(self, writer, state);
            }
            Type::Map(_, _) => map::MapSerializer::write_prefix_sync(self, writer, state),
            Type::Tuple(_) => tuple::TupleSerializer::write_prefix_sync(self, writer, state),
            #[cfg(feature = "extended-types")]
            Type::Nested(_) => nested::NestedSerializer::write_prefix_sync(self, writer, state),
            Type::Point => geo::PointSerializer::write_prefix_sync(self, writer, state),
            Type::Ring => geo::RingSerializer::write_prefix_sync(self, writer, state),
            Type::Polygon => geo::PolygonSerializer::write_prefix_sync(self, writer, state),
            Type::MultiPolygon => {
                geo::MultiPolygonSerializer::write_prefix_sync(self, writer, state);
            }
            Type::LowCardinality(_) => {
                low_cardinality::LowCardinalitySerializer::write_prefix_sync(self, writer, state);
            }
            Type::Object => object::ObjectSerializer::write_prefix_sync(self, writer, state),
            #[cfg(feature = "extended-types")]
            Type::QBit { .. } => qbit::QBitSerializer::write_prefix_sync(self, writer, state),
            #[cfg(feature = "extended-types")]
            Type::Variant(_) => variant::VariantSerializer::write_prefix_sync(self, writer, state),
            #[cfg(feature = "extended-types")]
            Type::Dynamic { .. } => {
                dynamic::DynamicSerializer::write_prefix_sync(self, writer, state);
            }
            #[cfg(feature = "extended-types")]
            Type::SimpleAggregateFunction { .. } | Type::AggregateFunction { .. } => {
                aggregate_function::AggregateFunctionSerializer::write_prefix_sync(
                    self, writer, state,
                );
            }
            _ => {}
        }
    }
}
