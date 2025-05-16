use chrono::NaiveDate;
use chrono_tz::Tz;
use clickhouse_native::prelude::*;
use clickhouse_native::{ColumnDefinition, Type};
use uuid::Uuid;

/// Test data for round trip
#[derive(Row, Debug, Default, PartialEq, Clone)]
#[clickhouse_native(schema = get_testrowall_schema)]
pub struct TestRowAll {
    id:                        u64,
    int8_col:                  i8,
    int16_col:                 i16,
    int32_col:                 i32,
    int64_col:                 i64,
    uint8_col:                 u8,
    uint16_col:                u16,
    uint32_col:                u32,
    uint64_col:                u64,
    uint128_col:               u128,
    uint256_col:               u256,
    date_col:                  Date,
    datetime_col:              DateTime,
    datetime64_col:            DateTime64<3>,
    #[cfg(feature = "rust_decimal")]
    decimal32_col:             rust_decimal::Decimal,
    #[cfg(not(feature = "rust_decimal"))]
    decimal32_col:             FixedPoint32<9>,
    #[cfg(feature = "rust_decimal")]
    decimal64_col:             rust_decimal::Decimal,
    #[cfg(not(feature = "rust_decimal"))]
    decimal64_col:             FixedPoint64<18>,
    #[cfg(feature = "rust_decimal")]
    decimal128_col:            rust_decimal::Decimal,
    #[cfg(not(feature = "rust_decimal"))]
    decimal128_col:            FixedPoint128<38>,
    decimal256_col:            FixedPoint256<64>,
    nullable_string_col:       Option<String>,
    nullable_int32_col:        Option<i32>,
    nullable_uint64_col:       Option<u64>,
    array_uint64_col:          Vec<u64>,
    array_string_col:          Vec<String>,
    array_nullable_int32_col:  Vec<Option<i32>>,
    array_nullable_string_col: Vec<Option<String>>,
    string_col:                String,
    fixed_string_col:          String, // FixedString(5) trimmed to String
    uuid_col:                  Uuid,
}

pub fn get_testrowall_schema() -> Vec<ColumnDefinition> {
    vec![
        ("id".to_string(), Type::UInt64, None),
        ("int8_col".to_string(), Type::Int8, None),
        ("int16_col".to_string(), Type::Int16, None),
        ("int32_col".to_string(), Type::Int32, None),
        ("int64_col".to_string(), Type::Int64, None),
        ("uint8_col".to_string(), Type::UInt8, None),
        ("uint16_col".to_string(), Type::UInt16, None),
        ("uint32_col".to_string(), Type::UInt32, None),
        ("uint64_col".to_string(), Type::UInt64, None),
        ("uint128_col".to_string(), Type::UInt128, None),
        ("uint256_col".to_string(), Type::UInt256, None),
        ("date_col".to_string(), Type::Date, None),
        ("datetime_col".to_string(), Type::DateTime(Tz::UTC), None),
        ("datetime64_col".to_string(), Type::DateTime64(3, Tz::UTC), None),
        ("decimal32_col".to_string(), Type::Decimal32(2), None),
        ("decimal64_col".to_string(), Type::Decimal64(8), None),
        ("decimal128_col".to_string(), Type::Decimal128(12), None),
        ("decimal256_col".to_string(), Type::Decimal256(64), None),
        ("nullable_string_col".to_string(), Type::Nullable(Box::new(Type::String)), None),
        ("nullable_int32_col".to_string(), Type::Nullable(Box::new(Type::Int32)), None),
        ("nullable_uint64_col".to_string(), Type::Nullable(Box::new(Type::UInt64)), None),
        ("array_uint64_col".to_string(), Type::Array(Box::new(Type::UInt64)), None),
        ("array_string_col".to_string(), Type::Array(Box::new(Type::String)), None),
        (
            "array_nullable_int32_col".to_string(),
            Type::Array(Box::new(Type::Nullable(Box::new(Type::Int32)))),
            None,
        ),
        (
            "array_nullable_string_col".to_string(),
            Type::Array(Box::new(Type::Nullable(Box::new(Type::String)))),
            None,
        ),
        ("string_col".to_string(), Type::String, None),
        ("fixed_string_col".to_string(), Type::FixedSizedString(5), None),
        ("uuid_col".to_string(), Type::Uuid, None),
    ]
}

/// # Panics
#[expect(clippy::too_many_lines)]
pub fn generate_test_block() -> Vec<TestRowAll> {
    use clickhouse_native::i256;

    vec![
        // Row 1: Basic values with all non-nullable fields populated and nullable fields Some
        TestRowAll {
            id: 1,
            int8_col: 8,
            int16_col: 16,
            int32_col: 32,
            int64_col: 64,
            uint8_col: 8,
            uint16_col: 16,
            uint32_col: 32,
            uint64_col: 64,
            uint128_col: 128,
            uint256_col: u256::from(i256::from(256i128)),
            date_col: Date::from(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
            datetime_col: DateTime::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(1_431_648_000, 0).unwrap(),
            )
            .unwrap(),
            datetime64_col: DateTime64::<3>::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(1_431_648_000, 0).unwrap(),
            )
            .unwrap(),
            #[cfg(feature = "rust_decimal")]
            decimal32_col: rust_decimal::Decimal::new(1234, 2), // 12.34
            #[cfg(not(feature = "rust_decimal"))]
            decimal32_col: FixedPoint32::<9>(1234), // 12.34
            #[cfg(feature = "rust_decimal")]
            decimal64_col: rust_decimal::Decimal::new(123_456, 4), // 12.3456
            #[cfg(not(feature = "rust_decimal"))]
            decimal64_col: FixedPoint64::<18>(123_456), // 12.34
            #[cfg(feature = "rust_decimal")]
            decimal128_col: rust_decimal::Decimal::new(12_345_678, 6), // 12.345678
            #[cfg(not(feature = "rust_decimal"))]
            decimal128_col: FixedPoint128::<38>(12_345_678), // 12.34
            decimal256_col: FixedPoint256::<64>::from(123i128),
            nullable_string_col: Some("Test String 1".to_string()),
            nullable_int32_col: Some(42),
            nullable_uint64_col: Some(424_242),
            array_uint64_col: vec![1, 2, 3, 4],
            array_string_col: vec!["one".to_string(), "two".to_string(), "three".to_string()],
            array_nullable_int32_col: vec![Some(1), Some(2), None],
            array_nullable_string_col: vec![Some("a".to_string()), Some("b".to_string()), None],
            string_col: "Regular String".to_string(),
            fixed_string_col: "Fixed".to_string(),
            uuid_col: Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap(),
        },
        // Row 2: Different values, some nullables are None
        TestRowAll {
            id: 2,
            int8_col: -8,
            int16_col: -16,
            int32_col: -32,
            int64_col: -64,
            uint8_col: 18,
            uint16_col: 1616,
            uint32_col: 323_232,
            uint64_col: 646_464,
            uint128_col: 128_128,
            uint256_col: u256::from(i256::from(512i128)),
            date_col: Date::from(NaiveDate::from_ymd_opt(2023, 2, 15).unwrap()),
            datetime_col: DateTime::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(1_582_911_293, 20).unwrap(),
            )
            .unwrap(),
            datetime64_col: DateTime64::<3>::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(1_582_911_293, 20).unwrap(),
            )
            .unwrap(),
            #[cfg(feature = "rust_decimal")]
            decimal32_col: rust_decimal::Decimal::new(-5678, 2), // -56.78
            #[cfg(not(feature = "rust_decimal"))]
            decimal32_col: FixedPoint32::<9>(-5678), // -56.78 (-5678 / 10^2)
            #[cfg(feature = "rust_decimal")]
            decimal64_col: rust_decimal::Decimal::new(-9_876_543, 5), // -98.76543
            #[cfg(not(feature = "rust_decimal"))]
            decimal64_col: FixedPoint64::<18>(-9876543), // -98.76543 (-9876543 / 10^5)
            #[cfg(feature = "rust_decimal")]
            decimal128_col: rust_decimal::Decimal::new(-876_543_210, 7), // -87.6543210
            #[cfg(not(feature = "rust_decimal"))]
            decimal128_col: FixedPoint128::<38>(-876543210), /* -87.6543210 (-876543210 /
                                                              * 10^7) */
            decimal256_col: FixedPoint256::<64>::from(-987i128),
            nullable_string_col: None,
            nullable_int32_col: Some(-99),
            nullable_uint64_col: None,
            array_uint64_col: vec![10, 20, 30],
            array_string_col: vec!["alpha".to_string(), "beta".to_string()],
            array_nullable_int32_col: vec![None],
            array_nullable_string_col: vec![None, Some("y".to_string()), None],
            string_col: "Another String Value".to_string(),
            fixed_string_col: "12345".to_string(),
            uuid_col: Uuid::parse_str("87654321-4321-8765-abcd-987654321000").unwrap(),
        },
        // Row 3: Edge cases and boundary values
        TestRowAll {
            id: 3,
            int8_col: i8::MAX,
            int16_col: i16::MAX,
            int32_col: i32::MAX,
            int64_col: i64::MAX,
            uint8_col: u8::MAX,
            uint16_col: u16::MAX,
            uint32_col: u32::MAX,
            uint64_col: u64::MAX,
            uint128_col: u128::MAX,
            uint256_col: u256([1; 32]),
            date_col: Date::from(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()),
            datetime_col: DateTime::from_chrono_infallible_utc(
                chrono::DateTime::<chrono::Utc>::MAX_UTC,
            ),
            datetime64_col: DateTime64::<3>::try_from(chrono::DateTime::<chrono::Utc>::MAX_UTC)
                .unwrap(),
            #[cfg(feature = "rust_decimal")]
            decimal32_col: rust_decimal::Decimal::new(123_456_789, 2), // 1234567.89 (9 digits)
            #[cfg(not(feature = "rust_decimal"))]
            decimal32_col: FixedPoint32::<9>(123_456_789), // 1234567.89
            #[cfg(feature = "rust_decimal")]
            decimal64_col: rust_decimal::Decimal::new(788_654, 6), // 0.788654
            #[cfg(not(feature = "rust_decimal"))]
            decimal64_col: FixedPoint64::<18>(788654), // 0.788654 (788654 / 10^6)
            #[cfg(feature = "rust_decimal")]
            decimal128_col: rust_decimal::Decimal::new(-676_543_654, 8), // -6.76543654
            #[cfg(not(feature = "rust_decimal"))]
            decimal128_col: FixedPoint128::<38>(-676543654), /* -6.76543654 (-676543654 /
                                                              * 10^8) */
            decimal256_col: FixedPoint256::<64>::MAX, // Consistent
            nullable_string_col: Some(String::new()), // Empty string
            nullable_int32_col: Some(0),
            nullable_uint64_col: Some(0),
            array_uint64_col: vec![u64::MAX],
            array_string_col: vec![String::new()], // Empty string in array
            array_nullable_int32_col: vec![Some(i32::MAX)],
            array_nullable_string_col: vec![Some(String::new()), None, Some("z".to_string())],
            string_col: "特殊字符和Unicode测试".to_string(), // Unicode test
            fixed_string_col: "0".to_string(),
            uuid_col: Uuid::nil(), // Nil UUID
        },
        // Row 4: Minimal values
        TestRowAll {
            id: 4,
            int8_col: i8::MIN,
            int16_col: i16::MIN,
            int32_col: i32::MIN,
            int64_col: i64::MIN,
            uint8_col: 0,
            uint16_col: 0,
            uint32_col: 0,
            uint64_col: 0,
            uint128_col: 0,
            uint256_col: u256([0; 32]),
            date_col: Date::from(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            datetime_col: DateTime::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap(),
            )
            .unwrap(),
            datetime64_col: DateTime64::<3>::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap(),
            )
            .unwrap(),
            #[cfg(feature = "rust_decimal")]
            decimal32_col: rust_decimal::Decimal::ZERO, // 0
            #[cfg(not(feature = "rust_decimal"))]
            decimal32_col: FixedPoint32::<9>(0), // 0
            #[cfg(feature = "rust_decimal")]
            decimal64_col: rust_decimal::Decimal::ZERO, // 0
            #[cfg(not(feature = "rust_decimal"))]
            decimal64_col: FixedPoint64::<18>(0), // 0
            #[cfg(feature = "rust_decimal")]
            decimal128_col: rust_decimal::Decimal::ZERO, // 0
            #[cfg(not(feature = "rust_decimal"))]
            decimal128_col: FixedPoint128::<38>(0), // 0
            decimal256_col: FixedPoint256::<64>(i256([0; 32])),
            nullable_string_col: Some("Just one more test".to_string()),
            nullable_int32_col: None,
            nullable_uint64_col: Some(u64::MAX),
            array_uint64_col: vec![], // Empty array
            array_string_col: vec!["only_one".to_string()],
            array_nullable_int32_col: vec![Some(i32::MIN)],
            array_nullable_string_col: vec![],
            string_col: "x".repeat(1000), // Long string
            fixed_string_col: "12".to_string(),
            uuid_col: Uuid::new_v4(), // Random UUID
        },
    ]
}
