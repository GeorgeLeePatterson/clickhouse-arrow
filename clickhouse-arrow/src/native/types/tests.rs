use std::io::Cursor;
use std::net::{Ipv4Addr, Ipv6Addr};

use chrono_tz::Tz;
use uuid::Uuid;

use super::Type;
use super::deserialize::ClickHouseNativeDeserializer;
use super::serialize::ClickHouseNativeSerializer;
use crate::formats::{DeserializerState, SerializerState};
use crate::{
    Date, Date32, DateTime, DynDateTime64, MultiPolygon, Point, Polygon, Result, Ring, Value, i256,
    u256,
};

async fn roundtrip_values(type_: &Type, values: &[Value]) -> Result<Vec<Value>> {
    let mut output = vec![];

    let mut state = SerializerState::default();
    type_.serialize_prefix_async(&mut output, &mut state).await?;
    type_.serialize_column(values.to_vec(), &mut output, &mut state).await?;
    let mut input = Cursor::new(output);
    let mut state = DeserializerState::default();
    type_.deserialize_prefix_async(&mut input, &mut state).await?;
    let deserialized = type_.deserialize_column(&mut input, values.len(), &mut state).await?;

    Ok(deserialized)
}

fn roundtrip_values_sync(type_: &Type, values: &[Value]) -> Result<Vec<Value>> {
    let mut output = vec![];

    let mut state = SerializerState::default();
    // Most types don't have a prefix, so we can skip this for sync roundtrips
    type_.serialize_column_sync(values.to_vec(), &mut output, &mut state)?;
    let mut input = Cursor::new(output);
    let mut state = DeserializerState::default();
    // Most types don't have a prefix, so we can skip this for sync roundtrips
    let deserialized = type_.deserialize_column_sync(&mut input, values.len(), &mut state)?;

    Ok(deserialized)
}

#[tokio::test]
async fn roundtrip_u8() {
    let values = &[Value::UInt8(12), Value::UInt8(24), Value::UInt8(30)];
    assert_eq!(&values[..], roundtrip_values(&Type::UInt8, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_u16() {
    let values = &[Value::UInt16(12), Value::UInt16(24), Value::UInt16(30000)];
    assert_eq!(&values[..], roundtrip_values(&Type::UInt16, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_u32() {
    let values = &[Value::UInt32(12), Value::UInt32(24), Value::UInt32(900_000)];
    assert_eq!(&values[..], roundtrip_values(&Type::UInt32, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_u64() {
    let values = &[Value::UInt64(12), Value::UInt64(24), Value::UInt64(9_000_000_000)];
    assert_eq!(&values[..], roundtrip_values(&Type::UInt64, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_u128() {
    let values = &[
        Value::UInt128(12),
        Value::UInt128(24),
        Value::UInt128(9_000_000_000),
        Value::UInt128(9_000_000_000_u128 * 9_000_000_000),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::UInt128, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_u256() {
    let values = &[Value::UInt256(u256([0u8; 32])), Value::UInt256(u256([7u8; 32]))];
    assert_eq!(&values[..], roundtrip_values(&Type::UInt256, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_i8() {
    let values = &[Value::Int8(12), Value::Int8(24), Value::Int8(30), Value::Int8(-30)];
    assert_eq!(&values[..], roundtrip_values(&Type::Int8, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_i16() {
    let values = &[Value::Int16(12), Value::Int16(24), Value::Int16(30000), Value::Int16(-30000)];
    assert_eq!(&values[..], roundtrip_values(&Type::Int16, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_i32() {
    let values =
        &[Value::Int32(12), Value::Int32(24), Value::Int32(900_000), Value::Int32(900_0000)];
    assert_eq!(&values[..], roundtrip_values(&Type::Int32, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_i64() {
    let values = &[
        Value::Int64(12),
        Value::Int64(24),
        Value::Int64(9_000_000_000),
        Value::Int64(-9_000_000_000),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::Int64, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_i128() {
    let values = &[
        Value::Int128(12),
        Value::Int128(24),
        Value::Int128(9_000_000_000),
        Value::Int128(9_000_000_000_i128 * 9_000_000_000),
        Value::Int128(-9_000_000_000_i128 * 9_000_000_000),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::Int128, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_i256() {
    let values = &[Value::Int256(i256([0u8; 32])), Value::Int256(i256([7u8; 32]))];
    assert_eq!(&values[..], roundtrip_values(&Type::Int256, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_f32() {
    let values = &[
        Value::Float32(1.0_f32),
        Value::Float32(0.0_f32),
        Value::Float32(100.0_f32),
        Value::Float32(100_000.0_f32),
        Value::Float32(1_000_000.0_f32),
        Value::Float32(-1_000_000.0_f32),
        Value::Float32(f32::NAN),
        Value::Float32(f32::INFINITY),
        Value::Float32(f32::NEG_INFINITY),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::Float32, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_f64() {
    let values = &[
        Value::Float64(1.0_f64),
        Value::Float64(0.0_f64),
        Value::Float64(100.0_f64),
        Value::Float64(100_000.0_f64),
        Value::Float64(1_000_000.0_f64),
        Value::Float64(-1_000_000.0_f64),
        Value::Float64(f64::NAN),
        Value::Float64(f64::INFINITY),
        Value::Float64(f64::NEG_INFINITY),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::Float64, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_d32() {
    let values = &[
        Value::Decimal32(5, 12),
        Value::Decimal32(5, 24),
        Value::Decimal32(5, 900_000),
        Value::Decimal32(5, -900_000),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::Decimal32(5), &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_d64() {
    let values = &[
        Value::Decimal64(5, 12),
        Value::Decimal64(5, 24),
        Value::Decimal64(5, 9_000_000_000),
        Value::Decimal64(5, -9_000_000_000),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::Decimal64(5), &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_d128() {
    let values = &[
        Value::Decimal128(5, 12),
        Value::Decimal128(5, 24),
        Value::Decimal128(5, 9_000_000_000),
        Value::Decimal128(5, 9_000_000_000_i128 * 9_000_000_000),
        Value::Decimal128(5, -9_000_000_000_i128 * 9_000_000_000),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::Decimal128(5), &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_d256() {
    let values = &[Value::Decimal256(5, i256([0u8; 32])), Value::Decimal256(5, i256([7u8; 32]))];
    assert_eq!(&values[..], roundtrip_values(&Type::Decimal256(5), &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_null_int() {
    let values = &[
        Value::UInt32(35),
        Value::UInt32(90),
        Value::Null,
        Value::UInt32(120),
        Value::UInt32(10000),
        Value::Null,
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Nullable(Box::new(Type::UInt32)), &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_string() {
    let values = &[
        Value::string(""),
        Value::string("t"),
        Value::string("test"),
        Value::string("TESTST"),
        Value::string("日本語"),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::String, &values[..]).await.unwrap());
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::FixedSizedString(32), &values[..]).await.unwrap()
    );
    assert_ne!(
        &values[..],
        roundtrip_values(&Type::FixedSizedString(3), &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_null_string() {
    let values = &[
        Value::string(""),
        Value::Null,
        Value::string("t"),
        Value::string("test"),
        Value::Null,
        Value::string("TESTST"),
        Value::string("日本語"),
        Value::Null,
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Nullable(Box::new(Type::String)), &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_object() {
    let obj = "{\"a\":\"a\"}";
    let values = &[Value::string(obj)];
    assert_eq!(&values[..], roundtrip_values(&Type::String, &values[..]).await.unwrap());

    let values = &[Value::Object(obj.as_bytes().to_vec())];
    assert_eq!(&values[..], roundtrip_values(&Type::Object, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_uuid() {
    let values = &[
        Value::Uuid(Uuid::from_u128(0)),
        Value::Uuid(Uuid::from_u128(1)),
        Value::Uuid(Uuid::from_u128(456_345_634_563_456)),
    ];
    assert_eq!(&values[..], roundtrip_values(&Type::Uuid, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_ipv4() {
    let values = &[Value::Ipv4(Ipv4Addr::UNSPECIFIED.into())];
    assert_eq!(&values[..], roundtrip_values(&Type::Ipv4, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_ipv6() {
    let values = &[Value::Ipv6(Ipv6Addr::UNSPECIFIED.into())];
    assert_eq!(&values[..], roundtrip_values(&Type::Ipv6, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_date() {
    let values = &[Value::Date(Date(0)), Value::Date(Date(3234)), Value::Date(Date(45345))];
    assert_eq!(&values[..], roundtrip_values(&Type::Date, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_datetime() {
    let values = &[
        Value::DateTime(DateTime(chrono_tz::UTC, 0)),
        Value::DateTime(DateTime(chrono_tz::UTC, 323_463_434)),
        Value::DateTime(DateTime(chrono_tz::UTC, 45_345_345)),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::DateTime(chrono_tz::UTC), &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_datetime64() {
    let values = &[
        Value::DateTime64(DynDateTime64(chrono_tz::UTC, 0, 3)),
        Value::DateTime64(DynDateTime64(chrono_tz::UTC, 32_346_345_634, 3)),
        Value::DateTime64(DynDateTime64(chrono_tz::UTC, 4_534_564_345, 3)),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::DateTime64(3, chrono_tz::UTC), &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_enum8() {
    let type_ = Type::Enum8(vec![("hello".into(), 0)]);
    let values = &[Value::Enum8("hello".into(), 0)];
    assert_eq!(&values[..], roundtrip_values(&type_, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_enum16() {
    let type_ = Type::Enum16(vec![("hello".into(), 0)]);
    let values = &[Value::Enum16("hello".into(), 0)];
    assert_eq!(&values[..], roundtrip_values(&type_, &values[..]).await.unwrap());
}

#[tokio::test]
async fn roundtrip_array() {
    let values = &[
        Value::Array(vec![]),
        Value::Array(vec![Value::UInt32(0)]),
        Value::Array(vec![Value::UInt32(1), Value::UInt32(2), Value::UInt32(3)]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Array(Box::new(Type::UInt32)), &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_array2() {
    let values = &[
        Value::Array(vec![Value::Array(vec![])]),
        Value::Array(vec![Value::Array(vec![Value::UInt32(1)])]),
        Value::Array(vec![
            Value::Array(vec![Value::UInt32(2)]),
            Value::Array(vec![Value::UInt32(3)]),
        ]),
        Value::Array(vec![
            Value::Array(vec![Value::UInt32(4), Value::UInt32(5)]),
            Value::Array(vec![Value::UInt32(6), Value::UInt32(7)]),
        ]),
        Value::Array(vec![Value::Array(vec![Value::UInt32(8)])]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Array(Box::new(Type::Array(Box::new(Type::UInt32)))), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_tuple() {
    let values = &[
        Value::Tuple(vec![Value::UInt32(1), Value::UInt16(2)]),
        Value::Tuple(vec![Value::UInt32(3), Value::UInt16(4)]),
        Value::Tuple(vec![Value::UInt32(4), Value::UInt16(5)]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Tuple(vec![Type::UInt32, Type::UInt16]), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_2tuple() {
    let values = &[
        Value::Tuple(vec![
            Value::UInt32(1),
            Value::Tuple(vec![Value::UInt32(1), Value::UInt16(2)]),
        ]),
        Value::Tuple(vec![
            Value::UInt32(3),
            Value::Tuple(vec![Value::UInt32(3), Value::UInt16(4)]),
        ]),
        Value::Tuple(vec![
            Value::UInt32(4),
            Value::Tuple(vec![Value::UInt32(4), Value::UInt16(5)]),
        ]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Tuple(vec![Type::UInt32, Type::Tuple(vec![Type::UInt32, Type::UInt16])]),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_array_tuple() {
    let values = &[
        Value::Array(vec![
            Value::Tuple(vec![Value::UInt32(1), Value::UInt16(2)]),
            Value::Tuple(vec![Value::UInt32(3), Value::UInt16(4)]),
        ]),
        Value::Array(vec![Value::Tuple(vec![Value::UInt32(5), Value::UInt16(6)])]),
        Value::Array(vec![]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::Tuple(vec![Type::UInt32, Type::UInt16]))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_tuple_array() {
    let values = &[
        Value::Tuple(vec![Value::Array(vec![]), Value::Array(vec![])]),
        Value::Tuple(vec![Value::Array(vec![Value::UInt32(1)]), Value::Array(vec![])]),
        Value::Tuple(vec![Value::Array(vec![]), Value::Array(vec![Value::UInt16(2)])]),
        Value::Tuple(vec![
            Value::Array(vec![Value::UInt32(3)]),
            Value::Array(vec![Value::UInt16(4)]),
        ]),
        Value::Tuple(vec![
            Value::Array(vec![Value::UInt32(5), Value::UInt32(6)]),
            Value::Array(vec![Value::UInt16(7), Value::UInt16(8)]),
        ]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Tuple(vec![
                Type::Array(Box::new(Type::UInt32)),
                Type::Array(Box::new(Type::UInt16))
            ]),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_array_nulls() {
    let values = &[
        Value::Array(vec![]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::UInt32(0), Value::Null]),
        Value::Array(vec![Value::Null, Value::UInt32(0)]),
        Value::Array(vec![
            Value::Null,
            Value::Null,
            Value::UInt32(1),
            Value::UInt32(2),
            Value::Null,
            Value::UInt32(3),
        ]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::Nullable(Box::new(Type::UInt32)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_map() {
    let values = &[
        Value::Map(vec![], vec![]),
        Value::Map(vec![Value::UInt32(1)], vec![Value::UInt16(2)]),
        Value::Map(vec![Value::UInt32(5), Value::UInt32(3)], vec![
            Value::UInt16(6),
            Value::UInt16(4),
        ]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Map(Box::new(Type::UInt32), Box::new(Type::UInt16)), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_string() {
    let values = &[
        Value::string(""),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("bcd"),
        Value::string("bcd2"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::LowCardinality(Box::new(Type::String)), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_string_array() {
    let values = &[
        Value::Array(vec![]),
        Value::Array(vec![Value::string("")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("bcd")]),
        Value::Array(vec![Value::string("bcd2")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::LowCardinality(Box::new(Type::String)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_string_map() {
    let values = &[
        Value::Map(vec![Value::string("")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("bcd")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("bcd2")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Map(
                Box::new(Type::LowCardinality(Box::new(Type::String))),
                Box::new(Type::UInt32)
            ),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_string_null() {
    let values = &[
        Value::string(""),
        Value::Null,
        Value::string("abc"),
        Value::string("abc"),
        Value::string("bcd"),
        Value::string("bcd2"),
        Value::Null,
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::Null,
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::Null,
        Value::string("abc"),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::String)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_array_null() {
    let values = &[
        Value::Array(vec![Value::string("")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("bcd")]),
        Value::Array(vec![Value::string("bcd2")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::LowCardinality(Box::new(Type::Nullable(Box::new(
                Type::String
            )))))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_nullable_low_cardinality() {
    let values = &[Value::String("active".into()), Value::Null, Value::String("inactive".into())];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Nullable(Box::new(Type::LowCardinality(Box::new(Type::String)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_array_null() {
    let values = &[
        Value::Array(vec![Value::string("")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("bcd")]),
        Value::Array(vec![Value::string("bcd2")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::Nullable(Box::new(Type::String)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_geo() {
    // Points
    let point = |x| Point([x, x + 2.0]);
    let values = &[Value::Point(point(1.0)), Value::Point(point(3.0))];
    assert_eq!(&values[..], roundtrip_values(&Type::Point, &values[..]).await.unwrap());
    // Ring
    let ring = |x| Ring(vec![point(x), point(2.0 * x)]);
    let values = &[Value::Ring(ring(1.0)), Value::Ring(ring(3.0))];
    assert_eq!(&values[..], roundtrip_values(&Type::Ring, &values[..]).await.unwrap());
    // Polygon
    let polygon = |x| Polygon(vec![ring(x), ring(2.0 * x)]);
    let values = &[Value::Polygon(polygon(1.0)), Value::Polygon(polygon(3.0))];
    assert_eq!(&values[..], roundtrip_values(&Type::Polygon, &values[..]).await.unwrap());
    // Multipolygon
    let multipolygon = |x| MultiPolygon(vec![polygon(x), polygon(2.0 * x)]);
    let values = &[Value::MultiPolygon(multipolygon(1.0)), Value::MultiPolygon(multipolygon(3.0))];
    assert_eq!(&values[..], roundtrip_values(&Type::MultiPolygon, &values[..]).await.unwrap());
}

#[test]
fn test_type_methods() {
    let t = Type::Array(Box::new(Type::String));
    assert_eq!(t.unarray(), Some(&Type::String));
    assert!(Type::String.unarray().is_none());
    assert_eq!(t.unwrap_array().unwrap(), &Type::String);
    assert!(Type::String.unwrap_array().is_err());

    let t = Type::Map(Box::new(Type::String), Box::new(Type::String));
    assert_eq!(t.unmap(), Some((&Type::String, &Type::String)));
    assert!(Type::String.unmap().is_none());
    assert_eq!(t.unwrap_map().unwrap(), (&Type::String, &Type::String));
    assert!(Type::String.unwrap_map().is_err());

    let t = Type::Tuple(vec![Type::String]);
    assert_eq!(t.untuple(), Some(&[Type::String] as &[_]));
    assert!(Type::String.untuple().is_none());
    assert_eq!(t.unwrap_tuple().unwrap(), &[Type::String] as &[_]);
    assert!(Type::String.unwrap_tuple().is_err());

    let t = Type::Nullable(Box::new(Type::String));
    assert_eq!(t.unnull(), Some(&Type::String));
    assert!(Type::String.unnull().is_none());

    let t = Type::Nullable(Box::new(Type::String));
    assert_eq!(&t.clone().into_nullable(), &t);
    assert_eq!(Type::String.into_nullable(), t);
}

#[test]
fn test_type_validate() {
    assert!(Type::Decimal32(100).validate().is_err());
    assert!(Type::Decimal128(100).validate().is_err());
    assert!(Type::Decimal256(100).validate().is_err());
    assert!(Type::DateTime64(100, Tz::UTC).validate().is_err());
    assert!(Type::LowCardinality(Box::new(Type::MultiPolygon)).validate().is_err());
    assert!(Type::LowCardinality(Box::new(Type::String)).validate().is_ok());
    assert!(Type::Tuple(vec![Type::String]).validate().is_ok());
    assert!(Type::Nullable(Box::new(Type::Nullable(Box::new(Type::String)))).validate().is_err());
    assert!(Type::Map(Box::new(Type::String), Box::new(Type::String)).validate().is_ok());
    assert!(Type::Map(Box::new(Type::Ipv4), Box::new(Type::String)).validate().is_err());
}

// Sync tests for sized deserializer coverage
#[test]
fn roundtrip_sized_int_types_sync() {
    let test_cases = vec![
        (Type::Int8, vec![Value::Int8(-128), Value::Int8(0), Value::Int8(127)]),
        (Type::Int16, vec![Value::Int16(-32768), Value::Int16(0), Value::Int16(32767)]),
        (Type::Int32, vec![
            Value::Int32(-2_147_483_648),
            Value::Int32(0),
            Value::Int32(2_147_483_647),
        ]),
        (Type::Int64, vec![Value::Int64(i64::MIN), Value::Int64(0), Value::Int64(i64::MAX)]),
        (Type::UInt8, vec![Value::UInt8(0), Value::UInt8(128), Value::UInt8(255)]),
        (Type::UInt16, vec![Value::UInt16(0), Value::UInt16(32768), Value::UInt16(65535)]),
        (Type::UInt32, vec![
            Value::UInt32(0),
            Value::UInt32(2_147_483_648),
            Value::UInt32(u32::MAX),
        ]),
        (Type::UInt64, vec![
            Value::UInt64(0),
            Value::UInt64(u64::from(u32::MAX) + 1),
            Value::UInt64(u64::MAX),
        ]),
    ];

    for (type_, values) in test_cases {
        let result = roundtrip_values_sync(&type_, &values);
        assert!(result.is_ok(), "Failed for type: {type_:?}");
        assert_eq!(values, result.unwrap());
    }
}

#[test]
fn roundtrip_sized_large_int_types_sync() {
    let test_cases = vec![
        (Type::Int128, vec![Value::Int128(-1), Value::Int128(0), Value::Int128(i128::MAX)]),
        (Type::Int256, vec![Value::Int256(i256([0u8; 32])), Value::Int256(i256([255u8; 32]))]),
        (Type::UInt128, vec![Value::UInt128(0), Value::UInt128(u128::MAX)]),
        (Type::UInt256, vec![Value::UInt256(u256([0u8; 32])), Value::UInt256(u256([255u8; 32]))]),
    ];

    for (type_, values) in test_cases {
        let result = roundtrip_values_sync(&type_, &values);
        assert!(result.is_ok(), "Failed for type: {type_:?}");
        assert_eq!(values, result.unwrap());
    }
}

#[test]
fn roundtrip_sized_float_types_sync() {
    let test_cases = vec![
        (Type::Float32, vec![
            Value::Float32(0.0),
            Value::Float32(3.15),
            Value::Float32(-1.0),
            Value::Float32(f32::NAN),
            Value::Float32(f32::INFINITY),
            Value::Float32(f32::NEG_INFINITY),
        ]),
        (Type::Float64, vec![
            Value::Float64(0.0),
            Value::Float64(3.15),
            Value::Float64(-1.0),
            Value::Float64(f64::NAN),
            Value::Float64(f64::INFINITY),
            Value::Float64(f64::NEG_INFINITY),
        ]),
    ];

    for (type_, values) in test_cases {
        let result = roundtrip_values_sync(&type_, &values);
        assert!(result.is_ok(), "Failed for type: {type_:?}");
        assert_eq!(values, result.unwrap());
    }
}

#[test]
fn roundtrip_sized_decimal_types_sync() {
    let test_cases = vec![
        (Type::Decimal32(2), vec![
            Value::Decimal32(2, -12345),
            Value::Decimal32(2, 0),
            Value::Decimal32(2, 12345),
        ]),
        (Type::Decimal64(4), vec![
            Value::Decimal64(4, -123_456_789),
            Value::Decimal64(4, 0),
            Value::Decimal64(4, 123_456_789),
        ]),
        (Type::Decimal128(6), vec![
            Value::Decimal128(6, -123_456_789_012_345),
            Value::Decimal128(6, 0),
            Value::Decimal128(6, 123_456_789_012_345),
        ]),
        (Type::Decimal256(8), vec![
            Value::Decimal256(8, i256([0u8; 32])),
            Value::Decimal256(8, i256([1u8; 32])),
        ]),
    ];

    for (type_, values) in test_cases {
        let result = roundtrip_values_sync(&type_, &values);
        assert!(result.is_ok(), "Failed for type: {type_:?}");
        assert_eq!(values, result.unwrap());
    }
}

#[test]
fn roundtrip_sized_date_time_types_sync() {
    use chrono_tz::UTC;
    let test_cases = vec![
        (Type::Date, vec![Value::Date(Date(0)), Value::Date(Date(18262))]), /* 1970-01-01 to
                                                                             * 2020-01-01 */
        (Type::Date32, vec![Value::Date32(Date32(0)), Value::Date32(Date32(-719_163))]), /* 1900-01-01 */
        (Type::DateTime(UTC), vec![
            Value::DateTime(DateTime(UTC, 0)),
            Value::DateTime(DateTime(UTC, 1_577_836_800)),
        ]), /* 1970-01-01 to 2020-01-01 */
        (Type::DateTime64(3, UTC), vec![
            Value::DateTime64(DynDateTime64(UTC, 0, 3)),
            Value::DateTime64(DynDateTime64(UTC, 1_577_836_800_000, 3)),
        ]),
    ];

    for (type_, values) in test_cases {
        let result = roundtrip_values_sync(&type_, &values);
        assert!(result.is_ok(), "Failed for type: {type_:?}");
        assert_eq!(values, result.unwrap());
    }
}

#[test]
fn roundtrip_sized_network_types_sync() {
    use crate::{Ipv4, Ipv6};
    let test_cases = vec![
        (Type::Ipv4, vec![
            Value::Ipv4(Ipv4(Ipv4Addr::UNSPECIFIED)),
            Value::Ipv4(Ipv4(Ipv4Addr::new(192, 168, 1, 1))),
            Value::Ipv4(Ipv4(Ipv4Addr::BROADCAST)),
        ]),
        (Type::Ipv6, vec![
            Value::Ipv6(Ipv6(Ipv6Addr::UNSPECIFIED)),
            Value::Ipv6(Ipv6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1))),
        ]),
    ];

    for (type_, values) in test_cases {
        let result = roundtrip_values_sync(&type_, &values);
        assert!(result.is_ok(), "Failed for type: {type_:?}");
        assert_eq!(values, result.unwrap());
    }
}

#[test]
fn roundtrip_sized_uuid_sync() {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let values = vec![Value::Uuid(uuid1), Value::Uuid(uuid2)];

    let result = roundtrip_values_sync(&Type::Uuid, &values);
    assert!(result.is_ok());
    assert_eq!(values, result.unwrap());
}

#[test]
fn roundtrip_sized_enum_types_sync() {
    let enum8_values =
        vec![("red".to_string(), 1i8), ("green".to_string(), 2i8), ("blue".to_string(), 3i8)];
    let enum16_values = vec![
        ("small".to_string(), 100i16),
        ("medium".to_string(), 200i16),
        ("large".to_string(), 300i16),
    ];

    let test_cases = vec![
        (Type::Enum8(enum8_values.clone()), vec![
            Value::Enum8("red".to_string(), 1),
            Value::Enum8("green".to_string(), 2),
            Value::Enum8("blue".to_string(), 3),
        ]),
        (Type::Enum16(enum16_values.clone()), vec![
            Value::Enum16("small".to_string(), 100),
            Value::Enum16("medium".to_string(), 200),
            Value::Enum16("large".to_string(), 300),
        ]),
    ];

    for (type_, values) in test_cases {
        let result = roundtrip_values_sync(&type_, &values);
        assert!(result.is_ok(), "Failed for type: {type_:?}");
        assert_eq!(values, result.unwrap());
    }
}

#[tokio::test]
async fn test_write_default() {
    let mut writer = Cursor::new(Vec::new());

    // Test all basic integer and numeric types
    assert!(Type::Int8.write_default(&mut writer).await.is_ok());
    assert!(Type::Int16.write_default(&mut writer).await.is_ok());
    assert!(Type::Int32.write_default(&mut writer).await.is_ok());
    assert!(Type::Int64.write_default(&mut writer).await.is_ok());
    assert!(Type::Int128.write_default(&mut writer).await.is_ok());
    assert!(Type::Int256.write_default(&mut writer).await.is_ok());
    assert!(Type::UInt8.write_default(&mut writer).await.is_ok());
    assert!(Type::UInt16.write_default(&mut writer).await.is_ok());
    assert!(Type::UInt32.write_default(&mut writer).await.is_ok());
    assert!(Type::UInt64.write_default(&mut writer).await.is_ok());
    assert!(Type::UInt128.write_default(&mut writer).await.is_ok());
    assert!(Type::UInt256.write_default(&mut writer).await.is_ok());
    assert!(Type::Float32.write_default(&mut writer).await.is_ok());
    assert!(Type::Float64.write_default(&mut writer).await.is_ok());

    // Test decimal types
    assert!(Type::Decimal32(2).write_default(&mut writer).await.is_ok());
    assert!(Type::Decimal64(4).write_default(&mut writer).await.is_ok());
    assert!(Type::Decimal128(6).write_default(&mut writer).await.is_ok());
    assert!(Type::Decimal256(8).write_default(&mut writer).await.is_ok());

    // Test string and binary types
    assert!(Type::String.write_default(&mut writer).await.is_ok());
    assert!(Type::Binary.write_default(&mut writer).await.is_ok());
    assert!(Type::FixedSizedString(10).write_default(&mut writer).await.is_ok());
    assert!(Type::FixedSizedBinary(16).write_default(&mut writer).await.is_ok());

    // Test date/time types
    assert!(Type::Date.write_default(&mut writer).await.is_ok());
    assert!(Type::Date32.write_default(&mut writer).await.is_ok());
    assert!(Type::DateTime(chrono_tz::UTC).write_default(&mut writer).await.is_ok());
    assert!(Type::DateTime64(3, chrono_tz::UTC).write_default(&mut writer).await.is_ok());

    // Test network types
    assert!(Type::Ipv4.write_default(&mut writer).await.is_ok());
    assert!(Type::Ipv6.write_default(&mut writer).await.is_ok());
    assert!(Type::Uuid.write_default(&mut writer).await.is_ok());

    // Test enum types
    assert!(Type::Enum8(vec![("test".to_string(), 1)]).write_default(&mut writer).await.is_ok());
    assert!(Type::Enum16(vec![("test".to_string(), 1)]).write_default(&mut writer).await.is_ok());

    // Test collection types
    assert!(Type::Array(Box::new(Type::UInt8)).write_default(&mut writer).await.is_ok());
    assert!(
        Type::Map(Box::new(Type::String), Box::new(Type::UInt32))
            .write_default(&mut writer)
            .await
            .is_ok()
    );

    // Test tuple type
    assert!(Type::Tuple(vec![Type::Int32, Type::String]).write_default(&mut writer).await.is_ok());

    // Test low cardinality
    assert!(Type::LowCardinality(Box::new(Type::String)).write_default(&mut writer).await.is_ok());

    // Test unsupported type (should return error)
    assert!(Type::Point.write_default(&mut writer).await.is_err());
}

#[test]
fn test_put_default() {
    let mut writer = Vec::new();

    // Test all basic integer and numeric types
    assert!(Type::Int8.put_default(&mut writer).is_ok());
    assert!(Type::Int16.put_default(&mut writer).is_ok());
    assert!(Type::Int32.put_default(&mut writer).is_ok());
    assert!(Type::Int64.put_default(&mut writer).is_ok());
    assert!(Type::Int128.put_default(&mut writer).is_ok());
    assert!(Type::Int256.put_default(&mut writer).is_ok());
    assert!(Type::UInt8.put_default(&mut writer).is_ok());
    assert!(Type::UInt16.put_default(&mut writer).is_ok());
    assert!(Type::UInt32.put_default(&mut writer).is_ok());
    assert!(Type::UInt64.put_default(&mut writer).is_ok());
    assert!(Type::UInt128.put_default(&mut writer).is_ok());
    assert!(Type::UInt256.put_default(&mut writer).is_ok());
    assert!(Type::Float32.put_default(&mut writer).is_ok());
    assert!(Type::Float64.put_default(&mut writer).is_ok());

    // Test decimal types
    assert!(Type::Decimal32(2).put_default(&mut writer).is_ok());
    assert!(Type::Decimal64(4).put_default(&mut writer).is_ok());
    assert!(Type::Decimal128(6).put_default(&mut writer).is_ok());
    assert!(Type::Decimal256(8).put_default(&mut writer).is_ok());

    // Test string and binary types
    assert!(Type::String.put_default(&mut writer).is_ok());
    assert!(Type::Binary.put_default(&mut writer).is_ok());
    assert!(Type::FixedSizedString(10).put_default(&mut writer).is_ok());
    assert!(Type::FixedSizedBinary(16).put_default(&mut writer).is_ok());

    // Test date/time types
    assert!(Type::Date.put_default(&mut writer).is_ok());
    assert!(Type::Date32.put_default(&mut writer).is_ok());
    assert!(Type::DateTime(chrono_tz::UTC).put_default(&mut writer).is_ok());
    assert!(Type::DateTime64(3, chrono_tz::UTC).put_default(&mut writer).is_ok());

    // Test network types
    assert!(Type::Ipv4.put_default(&mut writer).is_ok());
    assert!(Type::Ipv6.put_default(&mut writer).is_ok());
    assert!(Type::Uuid.put_default(&mut writer).is_ok());

    // Test enum types
    assert!(Type::Enum8(vec![("test".to_string(), 1)]).put_default(&mut writer).is_ok());
    assert!(Type::Enum16(vec![("test".to_string(), 1)]).put_default(&mut writer).is_ok());

    // Test collection types
    assert!(Type::Array(Box::new(Type::UInt8)).put_default(&mut writer).is_ok());
    assert!(
        Type::Map(Box::new(Type::String), Box::new(Type::UInt32)).put_default(&mut writer).is_ok()
    );

    // Test tuple type
    assert!(Type::Tuple(vec![Type::Int32, Type::String]).put_default(&mut writer).is_ok());

    // Test low cardinality
    assert!(Type::LowCardinality(Box::new(Type::String)).put_default(&mut writer).is_ok());

    // Test unsupported type (should return error)
    assert!(Type::Point.put_default(&mut writer).is_err());
}
