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
