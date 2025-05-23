use std::net::{Ipv4Addr, Ipv6Addr};

use chrono_tz::{Tz, UTC};
use indexmap::IndexMap;
use uuid::Uuid;

use super::Value;
use crate::{
    Bytes, Date, DateTime, DateTime64, FixedPoint32, FixedPoint64, FixedPoint128, FixedPoint256,
    FromSql, Ipv4, Ipv6, MultiPolygon, Point, Polygon, Ring, ToSql, Type, i256, u256,
};

fn roundtrip<T: FromSql + ToSql>(item: T, type_: &Type) -> T {
    let serialized = item.to_sql(Some(type_)).expect("failed to serialize");
    serialized.to_value(type_).expect("failed to deserialize")
}

#[test]
fn roundtrip_u8() {
    assert_eq!(0u8, roundtrip(0u8, &Type::UInt8));
    assert_eq!(5u8, roundtrip(5u8, &Type::UInt8));
}

#[test]
fn roundtrip_u16() {
    assert_eq!(0u16, roundtrip(0u16, &Type::UInt16));
    assert_eq!(5u16, roundtrip(5u16, &Type::UInt16));
}

#[test]
fn roundtrip_u32() {
    assert_eq!(0u32, roundtrip(0u32, &Type::UInt32));
    assert_eq!(5u32, roundtrip(5u32, &Type::UInt32));
}

#[test]
fn roundtrip_u64() {
    assert_eq!(0u64, roundtrip(0u64, &Type::UInt64));
    assert_eq!(5u64, roundtrip(5u64, &Type::UInt64));
}

#[test]
fn roundtrip_u128() {
    assert_eq!(0u128, roundtrip(0u128, &Type::UInt128));
    assert_eq!(5u128, roundtrip(5u128, &Type::UInt128));
}

#[test]
fn roundtrip_u256() {
    assert_eq!(u256::from((0u128, 0u128)), roundtrip(u256::from((0u128, 0u128)), &Type::UInt256));
    assert_eq!(u256::from((5u128, 0u128)), roundtrip(u256::from((5u128, 0u128)), &Type::UInt256));
}

#[test]
fn roundtrip_i8() {
    assert_eq!(0i8, roundtrip(0i8, &Type::Int8));
    assert_eq!(5i8, roundtrip(5i8, &Type::Int8));
    assert_eq!(-5i8, roundtrip(-5i8, &Type::Int8));
}

#[test]
fn roundtrip_i16() {
    assert_eq!(0i16, roundtrip(0i16, &Type::Int16));
    assert_eq!(5i16, roundtrip(5i16, &Type::Int16));
    assert_eq!(-5i16, roundtrip(-5i16, &Type::Int16));
}

#[test]
fn roundtrip_i32() {
    assert_eq!(0i32, roundtrip(0i32, &Type::Int32));
    assert_eq!(5i32, roundtrip(5i32, &Type::Int32));
    assert_eq!(-5i32, roundtrip(-5i32, &Type::Int32));
}

#[test]
fn roundtrip_i64() {
    assert_eq!(0i64, roundtrip(0i64, &Type::Int64));
    assert_eq!(5i64, roundtrip(5i64, &Type::Int64));
    assert_eq!(-5i64, roundtrip(-5i64, &Type::Int64));
}

#[test]
fn roundtrip_i128() {
    assert_eq!(0i128, roundtrip(0i128, &Type::Int128));
    assert_eq!(5i128, roundtrip(5i128, &Type::Int128));
    assert_eq!(-5i128, roundtrip(-5i128, &Type::Int128));
}

#[test]
fn roundtrip_i256() {
    assert_eq!(i256::from((0u128, 0u128)), roundtrip(i256::from((0u128, 0u128)), &Type::Int256));
    assert_eq!(i256::from((5u128, 0u128)), roundtrip(i256::from((5u128, 0u128)), &Type::Int256));
}

#[test]
fn roundtrip_f32() {
    const FLOATS: &[f32] = &[
        1.0_f32,
        0.0_f32,
        100.0_f32,
        100_000.0_f32,
        1_000_000.0_f32,
        -1_000_000.0_f32,
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
    ];

    const FIXED_POINTS: &[FixedPoint32<3>] =
        &[FixedPoint32::<3>(0), FixedPoint32::<3>(5), FixedPoint32::<3>(-5)];

    for float in FLOATS {
        assert_eq!(float.to_bits(), roundtrip(*float, &Type::Float32).to_bits());
    }

    for fixed in FIXED_POINTS {
        let float: f64 = f64::from(*fixed);
        let fixed_float =
            f64::from(fixed.integer()) + f64::from(fixed.fraction()) / f64::from(fixed.modulus());
        assert!((float - fixed_float) < 0.1_f64);
    }
}

#[test]
fn roundtrip_f64() {
    const FLOATS: &[f64] = &[
        1.0_f64,
        0.0_f64,
        100.0_f64,
        100_000.0_f64,
        1_000_000.0_f64,
        -1_000_000.0_f64,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];

    const FIXED_POINTS: &[FixedPoint64<3>] =
        &[FixedPoint64::<3>(0), FixedPoint64::<3>(5), FixedPoint64::<3>(-5)];

    for float in FLOATS {
        assert_eq!(float.to_bits(), roundtrip(*float, &Type::Float64).to_bits());
    }

    #[expect(clippy::cast_precision_loss)]
    for fixed in FIXED_POINTS {
        let float: f64 = f64::from(*fixed);
        let fixed_float = fixed.integer() as f64 + fixed.fraction() as f64 / fixed.modulus() as f64;
        assert!((float - fixed_float) < 0.1_f64);
    }
}

#[test]
fn roundtrip_d32() {
    assert_eq!(FixedPoint32::<3>(0), roundtrip(FixedPoint32::<3>(0), &Type::Decimal32(3)));
    assert_eq!(FixedPoint32::<3>(5), roundtrip(FixedPoint32::<3>(5), &Type::Decimal32(3)));
    assert_eq!(FixedPoint32::<3>(-5), roundtrip(FixedPoint32::<3>(-5), &Type::Decimal32(3)));
}

#[test]
fn roundtrip_d64() {
    assert_eq!(FixedPoint64::<3>(0), roundtrip(FixedPoint64::<3>(0), &Type::Decimal64(3)));
    assert_eq!(FixedPoint64::<3>(5), roundtrip(FixedPoint64::<3>(5), &Type::Decimal64(3)));
    assert_eq!(FixedPoint64::<3>(-5), roundtrip(FixedPoint64::<3>(-5), &Type::Decimal64(3)));
}

#[test]
fn roundtrip_d128() {
    assert_eq!(FixedPoint128::<3>(0), roundtrip(FixedPoint128::<3>(0), &Type::Decimal128(3)));
    assert_eq!(FixedPoint128::<3>(5), roundtrip(FixedPoint128::<3>(5), &Type::Decimal128(3)));
    assert_eq!(FixedPoint128::<3>(-5), roundtrip(FixedPoint128::<3>(-5), &Type::Decimal128(3)));
}

#[test]
fn roundtrip_d256() {
    let fixed = FixedPoint256::<3>(i256::from((0u128, 0u128)));
    assert_eq!(fixed, roundtrip(fixed, &Type::Decimal256(3)));
    let fixed = FixedPoint256::<3>(i256::from((5u128, 0u128)));
    assert_eq!(fixed, roundtrip(fixed, &Type::Decimal256(3)));
}

#[cfg(feature = "rust_decimal")]
#[test]
fn roundtrip_decimal() {
    let fixed = rust_decimal::Decimal::new(123_456, 4);
    assert_eq!(fixed, roundtrip(fixed, &Type::Decimal32(4)));
    let fixed = rust_decimal::Decimal::new(12_345_678, 6);
    assert_eq!(fixed, roundtrip(fixed, &Type::Decimal64(6)));
    let fixed = rust_decimal::Decimal::new(1_234_567_890, 8);
    assert_eq!(fixed, roundtrip(fixed, &Type::Decimal128(8)));
}

#[test]
fn roundtrip_string() {
    let fixed = "test".to_string();
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::String));
    let fixed = String::new();
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::String));
}

#[test]
fn roundtrip_fixed_string() {
    let fixed = "test".to_string();
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::FixedSizedString(32)));
    let fixed = String::new();
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::FixedSizedString(32)));
    let fixed = "test".to_string();
    // truncation happens at network layer serialization
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::FixedSizedString(3)));
}

#[test]
fn roundtrip_string_null() {
    let fixed = Some("test".to_string());
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::Nullable(Box::new(Type::String))));
    let fixed = Some(String::new());
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::Nullable(Box::new(Type::String))));
    let fixed = None::<String>;
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::Nullable(Box::new(Type::String))));
}

#[test]
fn roundtrip_uuid() {
    let fixed = Uuid::from_u128(0);
    assert_eq!(fixed, roundtrip(fixed, &Type::Uuid));
    let fixed = Uuid::from_u128(5);
    assert_eq!(fixed, roundtrip(fixed, &Type::Uuid));
}

#[test]
fn roundtrip_ipv4() {
    let fixed = Ipv4::from(Ipv4Addr::new(0, 0, 0, 0));
    assert_eq!(fixed, roundtrip(fixed, &Type::Ipv4));
}

#[test]
fn roundtrip_ipv6() {
    let fixed = Ipv6::from(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff));
    assert_eq!(fixed, roundtrip(fixed, &Type::Ipv6));
}

#[test]
fn roundtrip_bytes() {
    let fixed = Bytes(b"hello".to_vec());
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::String));
}

#[test]
fn roundtrip_bytes2() {
    let fixed = Bytes(b"hello".to_vec());
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::Array(Box::new(Type::UInt8))));
}

#[test]
fn roundtrip_date() {
    let fixed = Date(0);
    assert_eq!(fixed, roundtrip(fixed, &Type::Date));
    let fixed = Date(20000);
    assert_eq!(fixed, roundtrip(fixed, &Type::Date));
}

#[test]
fn roundtrip_datetime() {
    let fixed = DateTime(UTC, 0);
    assert_eq!(fixed, roundtrip(fixed, &Type::DateTime(UTC)));
    let fixed = DateTime(UTC, 323_463_434);
    assert_eq!(fixed, roundtrip(fixed, &Type::DateTime(UTC)));
    let fixed = DateTime(UTC, 45_345_345);
    assert_eq!(fixed, roundtrip(fixed, &Type::DateTime(UTC)));
}

#[test]
fn roundtrip_datetime64() {
    let fixed = DateTime64::<3>(UTC, 0);
    assert_eq!(fixed, roundtrip(fixed, &Type::DateTime64(3, UTC)));
    let fixed = DateTime64::<3>(UTC, 323_463_434);
    assert_eq!(fixed, roundtrip(fixed, &Type::DateTime64(3, UTC)));
    let fixed = DateTime64::<3>(UTC, 45_345_345);
    assert_eq!(fixed, roundtrip(fixed, &Type::DateTime64(3, UTC)));
}

#[cfg(feature = "serde")]
#[test]
fn roundtrip_json() {
    use crate::json::Json;

    let fixed = Json("hello".to_string());
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::Object));
}

#[test]
fn roundtrip_array() {
    let fixed = vec![5u32, 3, 2, 7];
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::Array(Box::new(Type::UInt32))));
    let fixed: Vec<u32> = vec![];
    assert_eq!(fixed, roundtrip(fixed.clone(), &Type::Array(Box::new(Type::UInt32))));
}

#[test]
fn roundtrip_2array() {
    let fixed =
        vec![vec![5u32, 3, 2, 7], vec![5u32, 3, 2, 7], vec![5u32, 3, 2, 7], vec![5u32, 3, 2, 7]];
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Array(Box::new(Type::Array(Box::new(Type::UInt32)))))
    );
    let fixed: Vec<Vec<u32>> = vec![];
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Array(Box::new(Type::Array(Box::new(Type::UInt32)))))
    );
    let fixed: Vec<Vec<u32>> = vec![vec![]];
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Array(Box::new(Type::Array(Box::new(Type::UInt32)))))
    );
    let fixed: Vec<Vec<u32>> = vec![vec![], vec![5u32, 3, 2, 7]];
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Array(Box::new(Type::Array(Box::new(Type::UInt32)))))
    );
}

#[test]
fn roundtrip_tuple() {
    let fixed = (5u32, 7u16);
    assert_eq!(fixed, roundtrip(fixed, &Type::Tuple(vec![Type::UInt32, Type::UInt16])));
    let fixed = (1_231_123_u32, 7123u16);
    assert_eq!(fixed, roundtrip(fixed, &Type::Tuple(vec![Type::UInt32, Type::UInt16])));
}

#[test]
fn roundtrip_2tuple() {
    let fixed = (5u32, (5u32, 7u16));
    assert_eq!(
        fixed,
        roundtrip(
            fixed,
            &Type::Tuple(vec![Type::UInt32, Type::Tuple(vec![Type::UInt32, Type::UInt16])])
        )
    );
    let fixed = (1_231_123_u32, (5u32, 7u16));
    assert_eq!(
        fixed,
        roundtrip(
            fixed,
            &Type::Tuple(vec![Type::UInt32, Type::Tuple(vec![Type::UInt32, Type::UInt16])])
        )
    );
}

#[test]
fn roundtrip_array_tuple() {
    let fixed = vec![(5u32, 7u16)];
    assert_eq!(
        fixed,
        roundtrip(
            fixed.clone(),
            &Type::Array(Box::new(Type::Tuple(vec![Type::UInt32, Type::UInt16])))
        )
    );
    let fixed: Vec<(u32, u16)> = vec![];
    assert_eq!(
        fixed,
        roundtrip(
            fixed.clone(),
            &Type::Array(Box::new(Type::Tuple(vec![Type::UInt32, Type::UInt16])))
        )
    );
    let fixed = vec![(5u32, 7u16), (1_231_123_u32, 7123u16)];
    assert_eq!(
        fixed,
        roundtrip(
            fixed.clone(),
            &Type::Array(Box::new(Type::Tuple(vec![Type::UInt32, Type::UInt16])))
        )
    );
}

#[test]
fn roundtrip_tuple_array() {
    let fixed: (Vec<u32>, Vec<u16>) = (vec![], vec![]);
    assert_eq!(
        fixed,
        roundtrip(
            fixed.clone(),
            &Type::Tuple(vec![
                Type::Array(Box::new(Type::UInt32)),
                Type::Array(Box::new(Type::UInt16))
            ])
        )
    );
    let fixed: (Vec<u32>, Vec<u16>) = (vec![5], vec![3]);
    assert_eq!(
        fixed,
        roundtrip(
            fixed.clone(),
            &Type::Tuple(vec![
                Type::Array(Box::new(Type::UInt32)),
                Type::Array(Box::new(Type::UInt16))
            ])
        )
    );
    let fixed: (Vec<u32>, Vec<u16>) = (vec![5, 3], vec![3, 2, 7]);
    assert_eq!(
        fixed,
        roundtrip(
            fixed.clone(),
            &Type::Tuple(vec![
                Type::Array(Box::new(Type::UInt32)),
                Type::Array(Box::new(Type::UInt16))
            ])
        )
    );
}

#[test]
fn roundtrip_array_nulls() {
    let fixed = vec![Some(5u32), None, Some(3), Some(2), None];
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Array(Box::new(Type::Nullable(Box::new(Type::UInt32)))))
    );
    let fixed: Vec<Option<u32>> = vec![None];
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Array(Box::new(Type::Nullable(Box::new(Type::UInt32)))))
    );
}

#[test]
fn roundtrip_map() {
    let mut fixed: IndexMap<String, String> = IndexMap::new();
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Map(Box::new(Type::String), Box::new(Type::String)))
    );
    drop(fixed.insert("test".to_string(), "value".to_string()));
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Map(Box::new(Type::String), Box::new(Type::String)))
    );
    drop(fixed.insert("t2est".to_string(), "v2alue".to_string()));
    assert_eq!(
        fixed,
        roundtrip(fixed.clone(), &Type::Map(Box::new(Type::String), Box::new(Type::String)))
    );
}

#[test]
fn test_escape() {
    assert_eq!(Value::string("test").to_string(), "'test'");
    assert_eq!(Value::string("te\nst").to_string(), "'te\\nst'");
    assert_eq!(Value::string("te\\nst").to_string(), "'te\\\\nst'");
    assert_eq!(Value::string("te\\xst").to_string(), "'te\\\\xst'");
    assert_eq!(Value::string("te'st").to_string(), "'te\\'st'");
    assert_eq!(Value::string("te\u{1F60A}st").to_string(), "'te\\xF0\\x9F\\x98\\x8Ast'");
}

#[tokio::test]
async fn roundtrip_geo() {
    // Points
    let point = Point([1.0, 2.0]);
    assert_eq!(&point, &roundtrip(point, &Type::Point));
    // Ring
    let ring = Ring(vec![point, Point([3.0, 4.0])]);
    assert_eq!(&ring, &roundtrip(ring.clone(), &Type::Ring));
    // Polygon
    let polygon = Polygon(vec![ring.clone(), Ring(vec![Point([5.0, 6.0])])]);
    assert_eq!(&polygon, &roundtrip(polygon.clone(), &Type::Polygon));
    // Multipolygon
    let multipolygon =
        MultiPolygon(vec![polygon.clone(), Polygon(vec![ring.clone(), Ring(vec![point])])]);
    assert_eq!(&multipolygon, &roundtrip(multipolygon.clone(), &Type::MultiPolygon));
}

#[test]
fn test_value_methods() {
    let inner = Value::String(b"hello".to_vec());
    let val = Value::Array(vec![inner.clone()]);
    assert_eq!(val.unwrap_array_ref().unwrap(), &[inner.clone()] as &[_]);
    assert_eq!(val.clone().unwrap_array().unwrap(), vec![inner.clone()]);
    assert_eq!(val.unarray().unwrap(), vec![inner.clone()]);
    assert!(Value::Int8(0).unwrap_array_ref().is_err());
    assert!(Value::Int8(0).unwrap_array().is_err());
    assert_eq!(Value::Int8(0).unarray(), None);
    assert_eq!(Value::from_value::<String>("hello".to_string()).unwrap(), inner.clone());

    let val = Value::Tuple(vec![inner.clone()]);
    assert_eq!(val.unwrap_tuple().unwrap(), vec![inner]);
    assert!(Value::Int8(0).unwrap_tuple().is_err());

    let val_types = [
        Type::Int8,
        Type::Int16,
        Type::Int32,
        Type::Int64,
        Type::Int128,
        Type::Int256,
        Type::UInt8,
        Type::UInt16,
        Type::UInt32,
        Type::UInt64,
        Type::UInt128,
        Type::UInt256,
        Type::Float32,
        Type::Float64,
        Type::Decimal32(0),
        Type::Decimal64(0),
        Type::Decimal128(0),
        Type::Decimal256(0),
        Type::String,
        Type::Uuid,
        Type::Date,
        Type::Date32,
        Type::DateTime(Tz::UTC),
        Type::DateTime64(3, Tz::UTC),
        Type::Array(Box::new(Type::String)),
        Type::Enum8(vec![(String::new(), 0)]),
        Type::Enum16(vec![(String::new(), 0)]),
        Type::Tuple(vec![Type::String]),
        Type::Map(Box::new(Type::String), Box::new(Type::String)),
        Type::Ipv4,
        Type::Ipv6,
        Type::Object,
    ];

    for type_ in val_types {
        let def_val = type_.default_value();
        assert_eq!(def_val.guess_type(), type_);
    }
}
