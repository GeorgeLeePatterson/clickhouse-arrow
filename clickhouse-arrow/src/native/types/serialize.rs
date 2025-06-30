pub(crate) mod array;
pub(crate) mod geo;
pub(crate) mod low_cardinality;
pub(crate) mod map;
pub(crate) mod nullable;
pub(crate) mod object;
pub(crate) mod sized;
pub(crate) mod string;
pub(crate) mod tuple;

use super::low_cardinality::LOW_CARDINALITY_VERSION;
use super::*;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};

pub(crate) trait ClickHouseNativeSerializer {
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
        let type_ = match self {
            Type::Nullable(inner) | Type::Array(inner) => inner,
            Type::Map(key, value) => &super::map::normalize_map_type(key, value),
            Type::Tuple(inner) => {
                for item in inner {
                    item.serialize_prefix(writer, state);
                }
                return;
            }
            Type::Point => {
                for _ in 0..2 {
                    Type::Float64.serialize_prefix(writer, state);
                }
                return;
            }
            Type::LowCardinality(_) => {
                writer.put_u64_le(LOW_CARDINALITY_VERSION);
                return;
            }
            Type::Object => {
                writer.put_i8(1);
                return;
            }
            _ => return,
        };
        type_.serialize_prefix(writer, state);
    }
}
