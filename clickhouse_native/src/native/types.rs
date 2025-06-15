pub(crate) mod deserialize;
pub mod geo;
pub(crate) mod low_cardinality;
pub(crate) mod serialize;
#[cfg(test)]
mod tests;

use std::fmt::Display;
use std::str::FromStr;

use chrono_tz::Tz;
use futures_util::FutureExt;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use super::protocol::MAX_STRING_SIZE;
use super::values::{
    Date, DateTime, DynDateTime64, Ipv4, Ipv6, MultiPolygon, Point, Polygon, Ring, Value, i256,
    u256,
};
use crate::formats::{DeserializerState, SerializerState};
use crate::io::{ClickhouseRead, ClickhouseWrite};
use crate::{Date32, Error, Result};

/// A raw Clickhouse type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Type {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    Float32,
    Float64,

    // Inner value is SCALE
    Decimal32(usize),
    Decimal64(usize),
    Decimal128(usize),
    Decimal256(usize),

    String,
    FixedSizedString(usize),
    Binary,
    FixedSizedBinary(usize),

    Uuid,

    Date,
    Date32, // NOTE: This is i32 days since 1900-01-01
    DateTime(Tz),
    DateTime64(usize, Tz),

    Ipv4,
    Ipv6,

    // Geo types, see
    // https://clickhouse.com/docs/en/sql-reference/data-types/geo
    // These are just aliases of primitive types.
    Point,
    Ring,
    Polygon,
    MultiPolygon,

    Nullable(Box<Type>),

    Enum8(Vec<(String, i8)>),
    Enum16(Vec<(String, i16)>),
    LowCardinality(Box<Type>),
    Array(Box<Type>),
    Tuple(Vec<Type>),
    Map(Box<Type>, Box<Type>),

    Object,
}

impl Type {
    /// # Errors
    ///
    /// Errors if the type is not an array
    pub fn unwrap_array(&self) -> Result<&Type> {
        match self {
            Type::Array(x) => Ok(x),
            _ => Err(Error::UnexpectedType(self.clone())),
        }
    }

    pub fn unarray(&self) -> Option<&Type> {
        match self {
            Type::Array(x) => Some(&**x),
            _ => None,
        }
    }

    /// # Errors
    ///
    /// Errors if the type is not a map
    pub fn unwrap_map(&self) -> Result<(&Type, &Type)> {
        match self {
            Type::Map(key, value) => Ok((&**key, &**value)),
            _ => Err(Error::UnexpectedType(self.clone())),
        }
    }

    pub fn unmap(&self) -> Option<(&Type, &Type)> {
        match self {
            Type::Map(key, value) => Some((&**key, &**value)),
            _ => None,
        }
    }

    /// # Errors
    ///
    /// Errors if the type is not a tuple
    pub fn unwrap_tuple(&self) -> Result<&[Type]> {
        match self {
            Type::Tuple(x) => Ok(&x[..]),
            _ => Err(Error::UnexpectedType(self.clone())),
        }
    }

    pub fn untuple(&self) -> Option<&[Type]> {
        match self {
            Type::Tuple(x) => Some(&x[..]),
            _ => None,
        }
    }

    pub fn unnull(&self) -> Option<&Type> {
        match self {
            Type::Nullable(x) => Some(&**x),
            _ => None,
        }
    }

    pub fn strip_null(&self) -> &Type {
        match self {
            Type::Nullable(x) => x,
            _ => self,
        }
    }

    pub fn is_nullable(&self) -> bool { matches!(self, Type::Nullable(_)) }

    pub fn strip_low_cardinality(&self) -> &Type {
        match self {
            Type::LowCardinality(x) => x,
            _ => self,
        }
    }

    #[must_use]
    pub fn into_nullable(self) -> Type {
        match self {
            t @ Type::Nullable(_) => t,
            // LowCardinality pushes nullability down
            Type::LowCardinality(inner) => Type::LowCardinality(Box::new(inner.into_nullable())),
            t => Type::Nullable(Box::new(t)),
        }
    }

    pub fn default_value(&self) -> Value {
        match self {
            Type::Int8 => Value::Int8(0),
            Type::Int16 => Value::Int16(0),
            Type::Int32 => Value::Int32(0),
            Type::Int64 => Value::Int64(0),
            Type::Int128 => Value::Int128(0),
            Type::Int256 => Value::Int256(i256::default()),
            Type::UInt8 => Value::UInt8(0),
            Type::UInt16 => Value::UInt16(0),
            Type::UInt32 => Value::UInt32(0),
            Type::UInt64 => Value::UInt64(0),
            Type::UInt128 => Value::UInt128(0),
            Type::UInt256 => Value::UInt256(u256::default()),
            Type::Float32 => Value::Float32(0.0),
            Type::Float64 => Value::Float64(0.0),
            Type::Decimal32(s) => Value::Decimal32(*s, 0),
            Type::Decimal64(s) => Value::Decimal64(*s, 0),
            Type::Decimal128(s) => Value::Decimal128(*s, 0),
            Type::Decimal256(s) => Value::Decimal256(*s, i256::default()),
            Type::String | Type::FixedSizedString(_) | Type::Binary | Type::FixedSizedBinary(_) => {
                Value::String(vec![])
            }
            Type::Date => Value::Date(Date(0)),
            Type::Date32 => Value::Date32(Date32(0)),
            Type::DateTime(tz) => Value::DateTime(DateTime(*tz, 0)),
            Type::DateTime64(precision, tz) => Value::DateTime64(DynDateTime64(*tz, 0, *precision)),
            Type::Ipv4 => Value::Ipv4(Ipv4::default()),
            Type::Ipv6 => Value::Ipv6(Ipv6::default()),
            Type::Enum8(_) => Value::Enum8(String::new(), 0),
            Type::Enum16(_) => Value::Enum16(String::new(), 0),
            Type::LowCardinality(x) => x.default_value(),
            Type::Array(_) => Value::Array(vec![]),
            Type::Tuple(types) => Value::Tuple(types.iter().map(Type::default_value).collect()),
            Type::Nullable(_) => Value::Null,
            Type::Map(_, _) => Value::Map(vec![], vec![]),
            Type::Point => Value::Point(Point::default()),
            Type::Ring => Value::Ring(Ring::default()),
            Type::Polygon => Value::Polygon(Polygon::default()),
            Type::MultiPolygon => Value::MultiPolygon(MultiPolygon::default()),
            Type::Uuid => Value::Uuid(Uuid::from_u128(0)),
            Type::Object => Value::Object("{}".as_bytes().to_vec()),
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Int8 => write!(f, "Int8"),
            Type::Int16 => write!(f, "Int16"),
            Type::Int32 => write!(f, "Int32"),
            Type::Int64 => write!(f, "Int64"),
            Type::Int128 => write!(f, "Int128"),
            Type::Int256 => write!(f, "Int256"),
            Type::UInt8 => write!(f, "UInt8"),
            Type::UInt16 => write!(f, "UInt16"),
            Type::UInt32 => write!(f, "UInt32"),
            Type::UInt64 => write!(f, "UInt64"),
            Type::UInt128 => write!(f, "UInt128"),
            Type::UInt256 => write!(f, "UInt256"),
            Type::Float32 => write!(f, "Float32"),
            Type::Float64 => write!(f, "Float64"),
            Type::Decimal32(s) => write!(f, "Decimal32({s})"),
            Type::Decimal64(s) => write!(f, "Decimal64({s})"),
            Type::Decimal128(s) => write!(f, "Decimal128({s})"),
            Type::Decimal256(s) => write!(f, "Decimal256({s})"),
            Type::String | Type::Binary => write!(f, "String"),
            Type::FixedSizedBinary(s) | Type::FixedSizedString(s) => write!(f, "FixedString({s})"),
            Type::Uuid => write!(f, "UUID"),
            Type::Date => write!(f, "Date"),
            Type::Date32 => write!(f, "Date32"),
            Type::DateTime(tz) => write!(f, "DateTime('{tz}')"),
            Type::DateTime64(precision, tz) => write!(f, "DateTime64({precision},'{tz}')"),
            Type::Ipv4 => write!(f, "IPv4"),
            Type::Ipv6 => write!(f, "IPv6"),
            Type::Point => write!(f, "Point"),
            Type::Ring => write!(f, "Ring"),
            Type::Polygon => write!(f, "Polygon"),
            Type::MultiPolygon => write!(f, "MultiPolygon"),
            Type::Enum8(items) => {
                write!(f, "Enum8(")?;
                if !items.is_empty() {
                    let last_index = items.len() - 1;
                    for (i, (name, value)) in items.iter().enumerate() {
                        write!(f, "'{}' = {value}", name.replace('\'', "''"))?;
                        if i < last_index {
                            write!(f, ",")?;
                        }
                    }
                }
                write!(f, ")")
            }
            Type::Enum16(items) => {
                write!(f, "Enum16(")?;
                if !items.is_empty() {
                    let last_index = items.len() - 1;
                    for (i, (name, value)) in items.iter().enumerate() {
                        write!(f, "'{}' = {value}", name.replace('\'', "''"))?;
                        if i < last_index {
                            write!(f, ",")?;
                        }
                    }
                }
                write!(f, ")")
            }
            Type::LowCardinality(inner) => write!(f, "LowCardinality({inner})"),
            Type::Array(inner) => write!(f, "Array({inner})"),
            Type::Tuple(items) => write!(
                f,
                "Tuple({})",
                items.iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
            ),
            Type::Nullable(inner) => write!(f, "Nullable({inner})"),
            Type::Map(key, value) => write!(f, "Map({key},{value})"),
            Type::Object => write!(f, "JSON"),
        }
    }
}

impl Type {
    pub(crate) fn deserialize_prefix<'a, R: ClickhouseRead>(
        &'a self,
        reader: &'a mut R,
        state: &'a mut DeserializerState,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use deserialize::*;
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
                    sized::SizedDeserializer::read_prefix(self, reader, state).await?;
                }

                Type::String
                | Type::FixedSizedString(_)
                | Type::Binary
                | Type::FixedSizedBinary(_) => {
                    string::StringDeserializer::read_prefix(self, reader, state).await?;
                }

                Type::Array(_) => {
                    array::ArrayDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Tuple(_) => {
                    tuple::TupleDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Point => geo::PointDeserializer::read_prefix(self, reader, state).await?,
                Type::Ring => geo::RingDeserializer::read_prefix(self, reader, state).await?,
                Type::Polygon => geo::PolygonDeserializer::read_prefix(self, reader, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Nullable(_) => {
                    nullable::NullableDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Map(_, _) => map::MapDeserializer::read_prefix(self, reader, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalityDeserializer::read_prefix(self, reader, state)
                        .await?;
                }
                Type::Object => {
                    object::ObjectDeserializer::read_prefix(self, reader, state).await?;
                }
            }
            Ok(())
        }
        .boxed()
    }

    pub(crate) fn deserialize_column<'a, R: ClickhouseRead>(
        &'a self,
        reader: &'a mut R,
        rows: usize,
        state: &'a mut DeserializerState,
    ) -> impl Future<Output = Result<Vec<Value>>> + Send + 'a {
        use deserialize::*;
        async move {
            if rows > MAX_STRING_SIZE {
                return Err(Error::Protocol(format!(
                    "deserialize response size too large. {rows} > {MAX_STRING_SIZE}"
                )));
            }

            Ok(match self {
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
                    sized::SizedDeserializer::read(self, reader, rows, state).await?
                }
                Type::String
                | Type::FixedSizedString(_)
                | Type::Binary
                | Type::FixedSizedBinary(_) => {
                    string::StringDeserializer::read(self, reader, rows, state).await?
                }
                Type::Array(_) => array::ArrayDeserializer::read(self, reader, rows, state).await?,
                Type::Ring => geo::RingDeserializer::read(self, reader, rows, state).await?,
                Type::Polygon => geo::PolygonDeserializer::read(self, reader, rows, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonDeserializer::read(self, reader, rows, state).await?
                }
                Type::Tuple(_) => tuple::TupleDeserializer::read(self, reader, rows, state).await?,
                Type::Point => geo::PointDeserializer::read(self, reader, rows, state).await?,
                Type::Nullable(_) => {
                    nullable::NullableDeserializer::read(self, reader, rows, state).await?
                }
                Type::Map(_, _) => map::MapDeserializer::read(self, reader, rows, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalityDeserializer::read(self, reader, rows, state)
                        .await?
                }
                Type::Object => object::ObjectDeserializer::read(self, reader, rows, state).await?,
            })
        }
        .boxed()
    }

    pub(crate) fn serialize_column<'a, W: ClickhouseWrite>(
        &'a self,
        values: Vec<Value>,
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
                    sized::SizedSerializer::write(self, values, writer, state).await?;
                }

                Type::String
                | Type::FixedSizedString(_)
                | Type::Binary
                | Type::FixedSizedBinary(_) => {
                    string::StringSerializer::write(self, values, writer, state).await?;
                }

                Type::Array(_) => {
                    array::ArraySerializer::write(self, values, writer, state).await?;
                }
                Type::Tuple(_) => {
                    tuple::TupleSerializer::write(self, values, writer, state).await?;
                }
                Type::Point => geo::PointSerializer::write(self, values, writer, state).await?,
                Type::Ring => geo::RingSerializer::write(self, values, writer, state).await?,
                Type::Polygon => geo::PolygonSerializer::write(self, values, writer, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonSerializer::write(self, values, writer, state).await?;
                }
                Type::Nullable(_) => {
                    nullable::NullableSerializer::write(self, values, writer, state).await?;
                }
                Type::Map(_, _) => map::MapSerializer::write(self, values, writer, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalitySerializer::write(self, values, writer, state)
                        .await?;
                }
                Type::Object => {
                    object::ObjectSerializer::write(self, values, writer, state).await?;
                }
            }
            Ok(())
        }
        .boxed()
    }

    pub(crate) fn serialize_prefix<'a, W: ClickhouseWrite>(
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

    #[expect(clippy::too_many_lines)]
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Type::Decimal32(scale) => {
                if *scale == 0 || *scale > 9 {
                    return Err(Error::TypeParseError(format!(
                        "scale out of bounds for Decimal32({}) must be in range (1..=9)",
                        *scale
                    )));
                }
            }

            Type::Decimal128(scale) => {
                if *scale == 0 || *scale > 38 {
                    return Err(Error::TypeParseError(format!(
                        "scale out of bounds for Decimal128({}) must be in range (1..=38)",
                        *scale
                    )));
                }
            }
            Type::Decimal256(scale) => {
                if *scale == 0 || *scale > 76 {
                    return Err(Error::TypeParseError(format!(
                        "scale out of bounds for Decimal256({}) must be in range (1..=76)",
                        *scale
                    )));
                }
            }
            Type::DateTime64(precision, _) | Type::Decimal64(precision) => {
                if *precision == 0 || *precision > 18 {
                    return Err(Error::TypeParseError(format!(
                        "precision out of bounds for Decimal64/DateTime64({}) must be in range \
                         (1..=18)",
                        *precision
                    )));
                }
            }
            Type::LowCardinality(inner) => match inner.strip_null() {
                Type::String
                | Type::FixedSizedString(_)
                | Type::Binary
                | Type::FixedSizedBinary(_)
                | Type::Date
                | Type::Date32
                | Type::DateTime(_)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Int8
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
                | Type::UInt256 => inner.validate()?,
                _ => {
                    return Err(Error::TypeParseError(format!(
                        "illegal type '{inner:?}' in LowCardinality, not allowed"
                    )));
                }
            },
            Type::Array(inner) => {
                inner.validate()?;
            }
            Type::Tuple(inner) => {
                for inner in inner {
                    inner.validate()?;
                }
            }
            Type::Nullable(inner) => match &**inner {
                Type::Array(_)
                | Type::Map(_, _)
                | Type::LowCardinality(_)
                | Type::Tuple(_)
                | Type::Nullable(_) => {
                    return Err(Error::TypeParseError(format!(
                        "nullable cannot contain composite type '{inner:?}'"
                    )));
                }
                _ => inner.validate()?,
            },
            Type::Map(key, value) => {
                if !matches!(
                    &**key,
                    Type::String
                        | Type::FixedSizedString(_)
                        | Type::Int8
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
                        | Type::LowCardinality(_)
                        | Type::Uuid
                        | Type::Date
                        | Type::DateTime(_)
                        | Type::DateTime64(_, _)
                        | Type::Enum8(_)
                        | Type::Enum16(_)
                ) {
                    return Err(Error::TypeParseError(
                        "key in map must be String, Integer, LowCardinality, FixedString, UUID, \
                         Date, DateTime, Date32, Enum"
                            .to_string(),
                    ));
                }
                key.validate()?;
                value.validate()?;
            }
            // TODO: Add Object
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn validate_value(&self, value: &Value) -> Result<()> {
        self.validate()?;
        if !self.inner_validate_value(value) {
            return Err(Error::TypeParseError(format!(
                "could not assign value '{value:?}' to type '{self:?}'"
            )));
        }
        Ok(())
    }

    fn inner_validate_value(&self, value: &Value) -> bool {
        match (self, value) {
            (Type::Int8, Value::Int8(_))
            | (Type::Int16, Value::Int16(_))
            | (Type::Int32, Value::Int32(_))
            | (Type::Int64, Value::Int64(_))
            | (Type::Int128, Value::Int128(_))
            | (Type::Int256, Value::Int256(_))
            | (Type::UInt8, Value::UInt8(_))
            | (Type::UInt16, Value::UInt16(_))
            | (Type::UInt32, Value::UInt32(_))
            | (Type::UInt64, Value::UInt64(_))
            | (Type::UInt128, Value::UInt128(_))
            | (Type::UInt256, Value::UInt256(_))
            | (Type::Float32, Value::Float32(_))
            | (Type::Float64, Value::Float64(_))
            | (Type::String | Type::FixedSizedString(_), Value::String(_))
            | (Type::Uuid, Value::Uuid(_))
            | (Type::Date, Value::Date(_))
            | (Type::Date32, Value::Date32(_))
            | (Type::Ipv4, Value::Ipv4(_))
            | (Type::Ipv6, Value::Ipv6(_))
            | (Type::Point, Value::Point(_))
            | (Type::Ring, Value::Ring(_))
            | (Type::Polygon, Value::Polygon(_))
            | (Type::MultiPolygon, Value::MultiPolygon(_)) => true,
            (Type::DateTime(tz1), Value::DateTime(date)) => tz1 == &date.0,
            (Type::DateTime64(precision1, tz1), Value::DateTime64(tz2)) => {
                tz1 == &tz2.0 && precision1 == &tz2.2
            }
            (Type::Decimal32(scale1), Value::Decimal32(scale2, _))
            | (Type::Decimal64(scale1), Value::Decimal64(scale2, _))
            | (Type::Decimal128(scale1), Value::Decimal128(scale2, _))
            | (Type::Decimal256(scale1), Value::Decimal256(scale2, _)) => scale1 >= scale2,
            (Type::FixedSizedString(_) | Type::String, Value::Array(items))
                if items.iter().all(|item| matches!(item, Value::UInt8(_) | Value::Int8(_))) =>
            {
                true
            }
            (Type::Enum8(entries), Value::Enum8(_, index)) => entries.iter().any(|x| x.1 == *index),
            (Type::Enum16(entries), Value::Enum16(_, index)) => {
                entries.iter().any(|x| x.1 == *index)
            }
            (Type::LowCardinality(x), value) => x.inner_validate_value(value),
            (Type::Array(inner_type), Value::Array(values)) => {
                values.iter().all(|x| inner_type.inner_validate_value(x))
            }
            (Type::Tuple(inner_types), Value::Tuple(values)) => {
                inner_types.len() == values.len()
                    && inner_types
                        .iter()
                        .zip(values.iter())
                        .all(|(type_, value)| type_.inner_validate_value(value))
            }
            (Type::Nullable(inner), value) => {
                value == &Value::Null || inner.inner_validate_value(value)
            }
            (Type::Map(key, value), Value::Map(keys, values)) => {
                keys.len() == values.len()
                    && keys.iter().all(|x| key.inner_validate_value(x))
                    && values.iter().all(|x| value.inner_validate_value(x))
            }
            _ => false,
        }
    }

    /// Helper type to estimate capacity of a type
    pub(crate) fn estimate_capacity(&self) -> usize {
        match self {
            Type::Int8 | Type::UInt8 => 1,
            Type::Int16 | Type::UInt16 | Type::Date => 2,
            Type::Int32
            | Type::UInt32
            | Type::Float32
            | Type::Date32
            | Type::DateTime(_)
            | Type::Ipv4 => 4,
            Type::Int64 | Type::UInt64 | Type::Float64 | Type::DateTime64(_, _) => 8,
            Type::Int128 | Type::UInt128 | Type::Uuid | Type::Ipv6 | Type::Point => 16,
            Type::Int256 | Type::UInt256 | Type::String | Type::Binary => 32,
            Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => *n,

            // Complex types
            Type::Array(inner) => {
                let inner_data = inner.estimate_capacity();
                (4 + inner_data) * 8 // 4 bytes for offsets estimate 8 items per array
            }
            Type::Nullable(inner) => inner.estimate_capacity(),
            Type::Tuple(types) => types.iter().map(Type::estimate_capacity).sum(),
            Type::Map(key, value) => {
                let key_data = key.estimate_capacity();
                let value_data = value.estimate_capacity();
                4 + key_data + value_data // 4 bytes for offsets
            }

            // Placeholder for unsupported types
            _ => 64, // Default to 8 bytes as a safe estimate
        }
    }
}

impl Type {
    /// Write a single default value based on 'non-null' type. This is useful in scenarios where
    /// `ClickHouse` expects a default value written for a null value of a type.
    ///
    /// # Errors
    /// Returns `SerializeError` if the `Type` is not handled.
    /// Returns `Io` error if the write fails.
    pub(crate) async fn write_default<W: ClickhouseWrite>(&self, writer: &mut W) -> Result<()> {
        match self.strip_null() {
            Type::String | Type::Binary => {
                writer.write_string("").await?;
            }
            Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => {
                writer.write_all(&vec![0u8; *n]).await?;
            }
            Type::Int8 => writer.write_i8(0).await?,
            Type::Int16 => writer.write_i16_le(0).await?,
            Type::Int32 => writer.write_i32_le(0).await?,
            Type::Int64 => writer.write_i64_le(0).await?,
            Type::Int128 => writer.write_all(&[0; 16]).await?,
            Type::Int256 => writer.write_all(&[0; 32]).await?,
            Type::UInt8 => writer.write_u8(0).await?,
            Type::UInt16 => writer.write_u16_le(0).await?,
            Type::UInt32 => writer.write_u32_le(0).await?,
            Type::UInt64 => writer.write_u64_le(0).await?,
            Type::UInt128 => writer.write_all(&[0; 16]).await?,
            Type::UInt256 => writer.write_all(&[0; 32]).await?,
            Type::Float32 => writer.write_f32_le(0.0).await?,
            Type::Float64 => writer.write_f64_le(0.0).await?,
            Type::Uuid => writer.write_all(&[0; 16]).await?,
            Type::Ipv4 => writer.write_u32_le(0).await?,
            Type::Ipv6 => writer.write_all(&[0; 16]).await?,
            Type::Date => writer.write_u16_le(0).await?,
            Type::Date32 => writer.write_i32_le(0).await?,
            Type::DateTime(_) => writer.write_u32_le(0).await?,
            Type::DateTime64(precision, _) => {
                let bytes = (0_i64).to_le_bytes();
                writer.write_all(&bytes[..*precision]).await?;
            }
            Type::Decimal32(_) => writer.write_i32_le(0).await?,
            Type::Decimal64(_) => writer.write_i64_le(0).await?,
            Type::Decimal128(_) => writer.write_all(&[0; 16]).await?,
            Type::Decimal256(_) => writer.write_all(&[0; 32]).await?,
            Type::Array(_) => writer.write_var_uint(0).await?, // Empty array
            Type::Map(_, _) => writer.write_var_uint(0).await?, // Empty map
            Type::Enum8(_) => writer.write_i8(0).await?,
            Type::Enum16(_) => writer.write_i16(0).await?,
            // Recursive
            Type::LowCardinality(inner) => Box::pin(inner.write_default(writer)).await?,
            Type::Tuple(inner) => {
                for t in inner {
                    Box::pin(t.write_default(writer)).await?;
                }
            }
            _ => {
                return Err(Error::SerializeError(format!("No default value for type: {self:?}")));
            }
        }
        Ok(())
    }
}

pub(crate) trait Deserializer {
    // TODO:
    // Add custom serialization here. Will need to pass in state or via arg.
    // Example from python:
    // ```
    //  def read_state_prefix(self, buf):
    //     if self.has_custom_serialization:
    //         use_custom_serialization = read_varint(buf)
    //         if use_custom_serialization:
    //             self.serialization = SparseSerialization(self)
    // ```
    fn read_prefix<R: ClickhouseRead>(
        _type_: &Type,
        _reader: &mut R,
        _state: &mut DeserializerState,
    ) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }

    fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> impl Future<Output = Result<Vec<Value>>>;
}

pub(crate) trait Serializer {
    fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }

    fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> impl Future<Output = Result<()>>;
}
