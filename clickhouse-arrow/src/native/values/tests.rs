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
    let fixed = Ipv4::from(Ipv4Addr::UNSPECIFIED);
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
    assert_eq!(val.unwrap_array_ref().unwrap(), std::slice::from_ref(&inner) as &[_]);
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

#[test]
fn test_value_partial_eq_same_types() {
    // Test basic equality for same types
    assert_eq!(Value::Int8(42), Value::Int8(42));
    assert_ne!(Value::Int8(42), Value::Int8(24));

    assert_eq!(Value::Float32(1.0), Value::Float32(1.0));
    assert_ne!(Value::Float32(1.0), Value::Float32(2.0));

    // Test NaN handling - NaN == NaN in Value (uses to_bits() comparison)
    assert_eq!(Value::Float32(f32::NAN), Value::Float32(f32::NAN));
    assert_eq!(Value::Float64(f64::NAN), Value::Float64(f64::NAN));

    // Test special float values
    assert_eq!(Value::Float32(f32::INFINITY), Value::Float32(f32::INFINITY));
    assert_eq!(Value::Float64(f64::NEG_INFINITY), Value::Float64(f64::NEG_INFINITY));

    // Test Decimal equality
    assert_eq!(Value::Decimal32(2, 123), Value::Decimal32(2, 123));
    assert_ne!(Value::Decimal32(2, 123), Value::Decimal32(3, 123)); // Different precision
    assert_ne!(Value::Decimal32(2, 123), Value::Decimal32(2, 124)); // Different value

    // Test Enum equality
    assert_eq!(Value::Enum8("test".to_string(), 42), Value::Enum8("test".to_string(), 42));
    assert_ne!(Value::Enum8("test".to_string(), 42), Value::Enum8("other".to_string(), 42));
    assert_ne!(Value::Enum8("test".to_string(), 42), Value::Enum8("test".to_string(), 24));

    // Test Map equality
    let map1 = Value::Map(vec![Value::String(b"key".to_vec())], vec![Value::Int32(42)]);
    let map2 = Value::Map(vec![Value::String(b"key".to_vec())], vec![Value::Int32(42)]);
    let map3 = Value::Map(vec![Value::String(b"key".to_vec())], vec![Value::Int32(24)]);
    assert_eq!(map1, map2);
    assert_ne!(map1, map3);
}

#[test]
fn test_value_partial_eq_cross_types() {
    // Test that different types are not equal (they should use discriminant comparison)
    assert_ne!(Value::Int8(42), Value::Int16(42));
    assert_ne!(Value::Int32(42), Value::UInt32(42));
    assert_ne!(Value::Float32(1.0), Value::Float64(1.0));
    assert_ne!(Value::String(b"test".to_vec()), Value::Object(b"test".to_vec()));

    // Test Null equality
    assert_eq!(Value::Null, Value::Null);
    assert_ne!(Value::Null, Value::Int32(0));
}

#[test]
fn test_value_hash_consistency() {
    use std::collections::HashMap;

    let mut map = HashMap::new();
    let _ = map.insert(Value::Int32(42), "test");
    let _ = map.insert(Value::String(b"hello".to_vec()), "world");
    let _ = map.insert(Value::Null, "null");

    // Test that we can retrieve values by hash
    assert_eq!(map.get(&Value::Int32(42)), Some(&"test"));
    assert_eq!(map.get(&Value::String(b"hello".to_vec())), Some(&"world"));
    assert_eq!(map.get(&Value::Null), Some(&"null"));

    // Test that different values have different hashes (usually)
    assert_ne!(map.get(&Value::Int32(24)), Some(&"test"));

    // Test float hash consistency (should use to_bits())
    let mut float_map = HashMap::new();
    let _ = float_map.insert(Value::Float32(1.0), "one");
    assert_eq!(float_map.get(&Value::Float32(1.0)), Some(&"one"));

    // NaN should be consistent with itself for hashing
    let _ = float_map.insert(Value::Float32(f32::NAN), "nan");
    assert_eq!(float_map.get(&Value::Float32(f32::NAN)), Some(&"nan"));
}

#[test]
fn test_value_display_formatting() {
    // Test integer display
    assert_eq!(Value::Int8(42).to_string(), "42");
    assert_eq!(Value::Int8(-42).to_string(), "-42");
    assert_eq!(Value::UInt64(12345).to_string(), "12345");

    // Test large integer types with suffix
    assert_eq!(Value::Int128(42).to_string(), "42::Int128");
    assert_eq!(
        Value::Int256(i256::from(42i128)).to_string(),
        "0x000000000000000000000000000000000000000000000000000000000000002A::Int256"
    );
    assert_eq!(Value::UInt128(42).to_string(), "42::UInt128");
    assert_eq!(
        Value::UInt256(u256::from((42u128, 0u128))).to_string(),
        "0x0000000000000000000000000000002A00000000000000000000000000000000::UInt256"
    );

    // Test float display
    assert_eq!(Value::Float32(1.5).to_string(), "1.5");
    assert_eq!(Value::Float64(-2.5).to_string(), "-2.5");
    assert_eq!(Value::Float32(f32::INFINITY).to_string(), "inf");
    assert_eq!(Value::Float64(f64::NEG_INFINITY).to_string(), "-inf");

    // Test decimal display with proper formatting
    assert_eq!(Value::Decimal32(2, 1234).to_string(), "12.34");
    assert_eq!(Value::Decimal32(4, 1234).to_string(), ".1234");
    assert_eq!(Value::Decimal64(3, 123_456).to_string(), "123.456");
    assert_eq!(Value::Decimal128(1, 42).to_string(), "4.2");
    assert_eq!(
        Value::Decimal256(0, i256::from(42i128)).to_string(),
        "0x000000000000000000000000000000000000000000000000000000000000002A."
    );

    // Test string escaping
    assert_eq!(Value::String(b"hello".to_vec()).to_string(), "'hello'");
    assert_eq!(Value::String(b"hello\nworld".to_vec()).to_string(), "'hello\\nworld'");
    assert_eq!(Value::String(b"hello\tworld".to_vec()).to_string(), "'hello\\tworld'");
    assert_eq!(Value::String(b"hello'world".to_vec()).to_string(), "'hello\\'world'");
    assert_eq!(Value::String(b"hello\\world".to_vec()).to_string(), "'hello\\\\world'");

    // Test UUID display
    let uuid = Uuid::from_u128(0x12345678_9abc_def0_1234_567890abcdef);
    assert_eq!(Value::Uuid(uuid).to_string(), format!("'{uuid}'"));

    // Test array display
    let arr = Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]);
    assert_eq!(arr.to_string(), "[1,2,3]");
    assert_eq!(Value::Array(vec![]).to_string(), "[]");

    // Test tuple display
    let tuple = Value::Tuple(vec![Value::Int32(1), Value::String(b"test".to_vec())]);
    assert_eq!(tuple.to_string(), "(1,'test')");
    assert_eq!(Value::Tuple(vec![]).to_string(), "()");

    // Test Null display
    assert_eq!(Value::Null.to_string(), "NULL");

    // Test Map display
    let map =
        Value::Map(vec![Value::String(b"key1".to_vec()), Value::String(b"key2".to_vec())], vec![
            Value::Int32(1),
            Value::Int32(2),
        ]);
    assert_eq!(map.to_string(), "{'key1':1,'key2':2}");
    assert_eq!(Value::Map(vec![], vec![]).to_string(), "{}");

    // Test Enum display
    assert_eq!(Value::Enum8("test".to_string(), 42).to_string(), "test");
    assert_eq!(Value::Enum16("value".to_string(), -1).to_string(), "value");

    // Test IP display
    let ipv4 = Ipv4::from(Ipv4Addr::new(192, 168, 1, 1));
    assert_eq!(Value::Ipv4(ipv4).to_string(), "'192.168.1.1'");

    // Test Object display
    assert_eq!(Value::Object(b"test".to_vec()).to_string(), "'test'");
}

#[test]
fn test_value_index_value() {
    // Test valid unsigned integer conversions
    assert_eq!(Value::UInt8(42).index_value().unwrap(), 42);
    assert_eq!(Value::UInt16(1000).index_value().unwrap(), 1000);
    assert_eq!(Value::UInt32(50000).index_value().unwrap(), 50000);
    assert_eq!(Value::UInt64(100_000).index_value().unwrap(), 100_000);

    // Test that non-unsigned integers return errors
    assert!(Value::Int8(42).index_value().is_err());
    assert!(Value::Int32(-42).index_value().is_err());
    assert!(Value::Float32(42.0).index_value().is_err());
    assert!(Value::String(b"42".to_vec()).index_value().is_err());
    assert!(Value::Null.index_value().is_err());
}

#[test]
fn test_value_unwrap_methods_errors() {
    // Test unwrap_array_ref errors
    let non_array = Value::Int32(42);
    assert!(non_array.unwrap_array_ref().is_err());

    // Test unwrap_array errors
    assert!(Value::String(b"test".to_vec()).unwrap_array().is_err());

    // Test unwrap_tuple errors
    assert!(Value::Array(vec![]).unwrap_tuple().is_err());
    assert!(Value::Int32(42).unwrap_tuple().is_err());

    // Test successful cases
    let array = Value::Array(vec![Value::Int32(1), Value::Int32(2)]);
    assert_eq!(array.unwrap_array_ref().unwrap().len(), 2);

    let tuple = Value::Tuple(vec![Value::Int32(1), Value::String(b"test".to_vec())]);
    assert_eq!(tuple.unwrap_tuple().unwrap().len(), 2);
}

#[test]
fn test_value_justify_null_ref() {
    use std::borrow::Cow;

    // Test non-null value returns borrowed reference
    let value = Value::Int32(42);
    let justified = value.justify_null_ref(&Type::Int32);
    match justified {
        Cow::Borrowed(v) => assert_eq!(*v, Value::Int32(42)),
        Cow::Owned(_) => panic!("Expected borrowed value"),
    }

    // Test null value returns owned default
    let null_value = Value::Null;
    let justified = null_value.justify_null_ref(&Type::Int32);
    match justified {
        Cow::Owned(v) => assert_eq!(v, Type::Int32.default_value()),
        Cow::Borrowed(_) => panic!("Expected owned value"),
    }
}

#[test]
fn test_value_guess_type_comprehensive() {
    // Test all basic types
    assert_eq!(Value::Int8(42).guess_type(), Type::Int8);
    assert_eq!(Value::UInt64(42).guess_type(), Type::UInt64);
    assert_eq!(Value::Float32(1.0).guess_type(), Type::Float32);
    assert_eq!(Value::String(b"test".to_vec()).guess_type(), Type::String);
    assert_eq!(Value::Null.guess_type(), Type::Nullable(Box::new(Type::String)));

    // Test decimal types preserve precision
    assert_eq!(Value::Decimal32(3, 123).guess_type(), Type::Decimal32(3));
    assert_eq!(Value::Decimal64(5, 12345).guess_type(), Type::Decimal64(5));

    // Test datetime types
    let dt = DateTime(UTC, 1_234_567_890);
    assert_eq!(Value::DateTime(dt).guess_type(), Type::DateTime(UTC));

    // Test enum types
    assert_eq!(
        Value::Enum8("test".to_string(), 42).guess_type(),
        Type::Enum8(vec![(String::new(), 42)])
    );
    assert_eq!(
        Value::Enum16("test".to_string(), -1).guess_type(),
        Type::Enum16(vec![(String::new(), -1)])
    );

    // Test array type inference
    let array_int = Value::Array(vec![Value::Int32(1), Value::Int32(2)]);
    assert_eq!(array_int.guess_type(), Type::Array(Box::new(Type::Int32)));

    // Test empty array defaults to String
    let empty_array = Value::Array(vec![]);
    assert_eq!(empty_array.guess_type(), Type::Array(Box::new(Type::String)));

    // Test tuple type inference
    let tuple = Value::Tuple(vec![Value::Int32(1), Value::String(b"test".to_vec())]);
    assert_eq!(tuple.guess_type(), Type::Tuple(vec![Type::Int32, Type::String]));

    // Test map type inference
    let map = Value::Map(vec![Value::String(b"key".to_vec())], vec![Value::Int32(42)]);
    assert_eq!(map.guess_type(), Type::Map(Box::new(Type::String), Box::new(Type::Int32)));

    // Test empty map defaults to String->String
    let empty_map = Value::Map(vec![], vec![]);
    assert_eq!(empty_map.guess_type(), Type::Map(Box::new(Type::String), Box::new(Type::String)));
}

#[test]
fn test_escape_string_comprehensive() {
    // Test escape_string functionality indirectly through Value::String display
    // since escape_string is a private helper function that takes a formatter

    // Test basic string
    assert_eq!(Value::String(b"hello".to_vec()).to_string(), "'hello'");

    // Test all escape sequences
    assert_eq!(Value::String(b"\\".to_vec()).to_string(), "'\\\\'");
    assert_eq!(Value::String(b"'".to_vec()).to_string(), "'\\''");
    assert_eq!(Value::String(b"\x08".to_vec()).to_string(), "'\\b'"); // backspace
    assert_eq!(Value::String(b"\x0C".to_vec()).to_string(), "'\\f'"); // form feed
    assert_eq!(Value::String(b"\r".to_vec()).to_string(), "'\\r'");
    assert_eq!(Value::String(b"\n".to_vec()).to_string(), "'\\n'");
    assert_eq!(Value::String(b"\t".to_vec()).to_string(), "'\\t'");
    assert_eq!(Value::String(b"\0".to_vec()).to_string(), "'\\0'");
    assert_eq!(Value::String(b"\x07".to_vec()).to_string(), "'\\a'"); // bell
    assert_eq!(Value::String(b"\x0B".to_vec()).to_string(), "'\\v'"); // vertical tab

    // Test high bytes (non-ASCII)
    assert_eq!(Value::String(vec![0xFF]).to_string(), "'\\xFF'");
    assert_eq!(Value::String(vec![0x80, 0x81]).to_string(), "'\\x80\\x81'");

    // Test mixed content
    assert_eq!(Value::String(b"hello\nworld\t!".to_vec()).to_string(), "'hello\\nworld\\t!'");

    // Test unicode emoji (should be escaped as bytes)
    assert_eq!(Value::String("🎉".as_bytes().to_vec()).to_string(), "'\\xF0\\x9F\\x8E\\x89'");
}

#[test]
fn test_value_string_constructor() {
    let value = Value::string("hello");
    assert_eq!(value, Value::String(b"hello".to_vec()));

    let value = Value::string(String::from("world"));
    assert_eq!(value, Value::String(b"world".to_vec()));

    let value = Value::string("");
    assert_eq!(value, Value::String(b"".to_vec()));
}

#[test]
fn test_value_to_from_value() {
    // Test to_value conversion
    let value = Value::Int32(42);
    let converted: i32 = value.to_value(&Type::Int32).unwrap();
    assert_eq!(converted, 42);

    // Test from_value conversion
    let value = Value::from_value(123i64).unwrap();
    assert_eq!(value, Value::Int64(123));

    // Test string conversions
    let value = Value::from_value("hello".to_string()).unwrap();
    assert_eq!(value, Value::String(b"hello".to_vec()));
}

#[test]
fn test_decimal_display_edge_cases() {
    // Test when raw value length is less than scale
    assert_eq!(Value::Decimal32(5, 123).to_string(), "123");
    assert_eq!(Value::Decimal64(10, 456).to_string(), "456");
    assert_eq!(Value::Decimal128(8, 789).to_string(), "789");
    assert_eq!(
        Value::Decimal256(6, i256::from(42i128)).to_string(),
        "0x0000000000000000000000000000000000000000000000000000000000.00002A"
    );

    // Test zero scale
    assert_eq!(Value::Decimal32(0, 123).to_string(), "123.");

    // Test negative decimals
    assert_eq!(Value::Decimal32(2, -1234).to_string(), "-12.34");
    assert_eq!(Value::Decimal64(1, -456).to_string(), "-45.6");
}

#[test]
fn test_datetime_display_error_handling() {
    // Test invalid datetime values that might cause display errors
    let datetime = DateTime(UTC, 0);
    let value = Value::DateTime(datetime);
    // This should not panic
    let _display = value.to_string();

    // Test DateTime64 with various precisions
    let dt64 = DateTime64::<3>(UTC, 1_234_567_890);
    let value = Value::DateTime64(dt64.into());
    let display = value.to_string();
    assert!(display.contains("parseDateTime64BestEffort"));
    assert!(display.contains(", 3)"));
}
