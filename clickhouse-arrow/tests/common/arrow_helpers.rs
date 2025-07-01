use std::collections::HashMap;
use std::f64;
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::arrow::block::{
    MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME, TUPLE_FIELD_NAME_PREFIX,
};
use uuid::Uuid;

pub type EnumMap<T> = HashMap<String, Vec<(String, T)>>;

pub mod assertions {
    //! # Round Trip (De)serialization
    //!
    //! The following functions aim to isolate instances of `exceptions` to round trip consistency.
    //!
    //! Use this as an organization point to keep track of these and as a reference to update
    //! documentation over time and in some cases provide workarounds.
    use arrow::array::*;
    use arrow::datatypes::*;
    use clickhouse_arrow::arrow::utils::array_to_string_iter;

    /// Macro to provide matching against common schema divergences
    #[macro_export]
    macro_rules! roundtrip_exceptions {
        (
            ($dt1:expr, $dt2:expr) => {
                $( dict($k1:pat, $v1:pat, $k2:pat, $v2:pat) => { $dictst:expr }; )?
                $( list($field1:pat, $field2:pat) => { $listst:expr }; )?
                $( utc_default() => { $utcst:expr }; )?
            };
            _ => $dflt:expr
        ) => {
            match ($dt1, $dt2) {
                // LowCardinality/Dictionaries
                $((DataType::Dictionary($k1, $v1), DataType::Dictionary($k2, $v2)) => {
                    $dictst
                })?
                // List LowCardinality/Dictionaries
                $((
                    DataType::List($field1)
                    | DataType::LargeList($field1)
                    | DataType::ListView($field1),
                    DataType::List($field2)
                    | DataType::LargeList($field2)
                    | DataType::ListView($field2),
                ) => {
                    $listst
                })?
                // Dates
                $((
                    DataType::Timestamp(TimeUnit::Millisecond, Some(tz)),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                ) if tz.as_ref() == "UTC" => {
                    $utcst
                })?
                // Default
                _ => {
                    $dflt
                },
            }
        };
    }

    // Macro to compare dictionaries
    macro_rules! compare_dictionaries {
        ($idx:expr, $left:expr, $right:expr, ($k1:expr, $v1:expr), $(
            [$dt1:pat, $dt2:pat, $key:ty, $val:ty]
        ),* $(,)?) => {{
            match ($k1, $v1) {
                $(
                    ($dt1, $dt2) => {
                        assert_eq!(
                            $left.as_any()
                                .downcast_ref::<DictionaryArray<$key>>()
                                .unwrap()
                                .downcast_dict::<$val>()
                                .unwrap()
                                .into_iter()
                                .collect::<Vec<_>>(),
                            $right.as_any()
                                .downcast_ref::<DictionaryArray<$key>>()
                                .unwrap()
                                .downcast_dict::<$val>()
                                .unwrap()
                                .into_iter()
                                .collect::<Vec<_>>(),
                            "Dictionary values mismatch: col={}, key={}, value={}",
                            $idx, stringify!($key), stringify!($val)
                        );
                        assert_eq!(
                            $left.logical_null_count(),
                            $right.logical_null_count(),
                            "Dictionary null count mismatch: col={}, key={}, value={}",
                            $idx, stringify!($key), stringify!($val)
                        );
                    }
                )*
                _ => panic!("Expected dictionary type: {}", $idx)
            }
        }}
    }

    /// # Panics
    pub fn compare_schemas(sch1: &SchemaRef, sch2: &SchemaRef) {
        let schema_fields = sch1.fields().iter().zip(sch2.fields().iter());

        for (sch_field1, sch_field2) in schema_fields {
            assert_eq!(sch_field1.name(), sch_field2.name());

            crate::roundtrip_exceptions!(
                (sch_field1.data_type(), sch_field2.data_type()) => {
                    dict(k1, v1, k2, v2) => {{
                        assert!(
                            sch_field1.is_nullable() == sch_field2.is_nullable()
                            && (k1.is_integer() && k2.is_integer())
                            && (
                                is_string_like(v1, v2)
                                || is_list_like(v1, v2)
                                || v1 == v2
                            )
                        );
                    }};
                    list(field1, field2) => {{
                        assert!(
                            field1.is_nullable() == field2.is_nullable()
                            && is_string_like(field1.data_type(), field2.data_type())
                            || is_list_like(field1.data_type(), field2.data_type())
                            || field1.data_type() == field2.data_type()
                        );
                    }};
                    utc_default() => {{
                        // The match is enough
                    }};
                };
                _ => { assert_eq!(sch_field1, sch_field2, "Schema fields mismatch"); }
            );
        }
    }

    fn is_string_like(dt1: &DataType, dt2: &DataType) -> bool {
        matches!(
            dt1,
            DataType::Utf8
                | DataType::Utf8View
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::BinaryView
                | DataType::LargeBinary
        ) && matches!(
            dt2,
            DataType::Utf8
                | DataType::Utf8View
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::BinaryView
                | DataType::LargeBinary
        )
    }

    fn is_list_like(dt1: &DataType, dt2: &DataType) -> bool {
        matches!(
            dt1,
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
        ) && matches!(
            dt2,
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
        )
    }

    // Dictionaries do not round-trip precisely so we compare their resolved values and nulls
    // NOTE: Currently only covers the schemas/values used in the round trip tests themselves.
    pub fn assert_dictionaries(
        i: usize,
        col: &ArrayRef,
        ins: &ArrayRef,
        k1: &DataType,
        v1: &DataType,
    ) {
        compare_dictionaries!(
            i,
            col,
            ins,
            (k1, v1),
            [DataType::Int8, DataType::Utf8, Int8Type, StringArray],
            [DataType::Int16, DataType::Utf8, Int16Type, StringArray],
            [DataType::Int32, DataType::Utf8, Int32Type, StringArray],
            [DataType::Int64, DataType::Utf8, Int64Type, StringArray],
        );
    }

    // - Lists may start as LargeListArray, for example, but will come back as ListArray
    // - Lists of dictionaries have the same note as above, but on the list's inner dictionaries.
    /// # Panics
    #[expect(clippy::single_match_else)]
    pub fn assert_lists(i: usize, col: &ArrayRef, ins: &ArrayRef, f1: &FieldRef, f2: &FieldRef) {
        match (f1.data_type(), f2.data_type()) {
            (DataType::Dictionary(k1, v1), DataType::Dictionary(_, _)) => {
                let left = col.as_any().downcast_ref::<ListArray>().unwrap().values();
                let right = ins.as_any().downcast_ref::<ListArray>().unwrap().values();
                assert_dictionaries(i, left, right, k1, v1);
            }
            _ => {
                let left = array_to_string_iter(col).unwrap().collect::<Vec<_>>();
                let right = array_to_string_iter(ins).unwrap().collect::<Vec<_>>();
                assert_eq!(left, right, "Column {i} mismatch");
            }
        }
    }

    // ClickHouse defaults to UTC, but a timestamp may have no tz specified.
    /// # Panics
    pub fn assert_datetimes_utf_default(col: &ArrayRef, ins: &ArrayRef) {
        assert_eq!(
            col.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().values(),
            ins.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().values(),
            "(values mismatch)",
        );
        assert_eq!(col.nulls(), ins.nulls(), "(nulls mismatch)");
    }
}

// TODO: Remove - add geo types
#[expect(clippy::too_many_lines)]
pub fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // Primitives
        Field::new("id", DataType::Int32, false),
        Field::new("int8_col", DataType::Int8, true),
        Field::new("int16_col", DataType::Int16, true),
        Field::new("int32_col", DataType::Int32, true),
        Field::new("int64_col", DataType::Int64, true),
        Field::new("uint8_col", DataType::UInt8, true),
        Field::new("uint16_col", DataType::UInt16, true),
        Field::new("uint32_col", DataType::UInt32, true),
        Field::new("uint64_col", DataType::UInt64, true),
        Field::new("int128_col", DataType::FixedSizeBinary(16), true),
        Field::new("uint128_col", DataType::FixedSizeBinary(16), true),
        Field::new("int256_col", DataType::FixedSizeBinary(32), true),
        Field::new("uint256_col", DataType::FixedSizeBinary(32), true),
        Field::new("float32_col", DataType::Float32, true),
        Field::new("float64_col", DataType::Float64, true),
        // String
        Field::new("string_col", DataType::Utf8, true),
        Field::new("fixed_string_col", DataType::Utf8, true),
        // Decimal
        Field::new("decimal32_col", DataType::Decimal128(9, 2), true),
        Field::new("decimal64_col", DataType::Decimal128(18, 4), true),
        // Datetimes
        Field::new("date_col", DataType::Date32, true),
        Field::new("date32_col", DataType::Date32, true),
        Field::new("datetime_col", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new(
            "datetime_utc_col",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "datetime_est_col",
            DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into())),
            true,
        ),
        Field::new(
            "datetime64_3_ny_col",
            DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into())),
            true,
        ),
        Field::new(
            "datetime64_6_tokyo_col",
            DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Tokyo".into())),
            true,
        ),
        Field::new(
            "datetime64_9_utc_col",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        // Maps and Tuples
        Field::new(
            "map_string_int32_col",
            DataType::Map(
                Arc::new(Field::new(
                    MAP_FIELD_NAME,
                    DataType::Struct(
                        vec![
                            Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false),
                            Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, false),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
        Field::new(
            "tuple_int32_string_col",
            DataType::Struct(
                vec![
                    Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
                    Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        ),
        // Special
        Field::new("ipv4_col", DataType::UInt32, true),
        Field::new("ipv6_col", DataType::FixedSizeBinary(16), true),
        Field::new("uuid_col", DataType::FixedSizeBinary(16), true),
        //
        // TODO
        // Field::new("json_col", DataType::Binary, true),
        // Field::new("point_col", DataType::Binary, true),
        //
        // Enums
        Field::new(
            "enum8_col",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "enum8_int32_col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "enum16_col",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
        ),
        // LowCardinality
        Field::new(
            "low_cardinality_string_col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "low_cardinality_nullable_string_col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        // Arrays
        Field::new(
            "array_low_cardinality_string_col",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        ),
        Field::new(
            "array_int32_col",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        ),
        Field::new(
            "array_nullable_int32_col",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ),
        Field::new(
            "array_nullable_string_col",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "large_list_int32_col",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        ),
        Field::new(
            "array_tuple_col",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
                        Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ))),
            false,
        ),
    ]))
}

// TODO: Support json column
// TODO: Support geo columns
/// # Panics
#[expect(clippy::too_many_lines)]
pub fn test_record_batch() -> RecordBatch {
    // Primitives
    let id = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let int8_col = Arc::new(Int8Array::from(vec![Some(0), None, Some(-128), Some(127), Some(42)]));
    let int16_col =
        Arc::new(Int16Array::from(vec![Some(0), None, Some(-32768), Some(32767), Some(1000)]));
    let int32_col = Arc::new(Int32Array::from(vec![
        Some(0),
        None,
        Some(i32::MIN),
        Some(i32::MAX),
        Some(1_000_000),
    ]));
    let int64_col = Arc::new(Int64Array::from(vec![
        Some(0),
        None,
        Some(i64::MIN),
        Some(i64::MAX),
        Some(1_000_000_000),
    ]));
    let uint8_col = Arc::new(UInt8Array::from(vec![Some(0), None, Some(0), Some(255), Some(128)]));
    let uint16_col =
        Arc::new(UInt16Array::from(vec![Some(0), None, Some(0), Some(65535), Some(50000)]));
    let uint32_col =
        Arc::new(UInt32Array::from(vec![Some(0), None, Some(0), Some(u32::MAX), Some(1_000_000)]));
    let uint64_col = Arc::new(UInt64Array::from(vec![
        Some(0),
        None,
        Some(0),
        Some(u64::MAX),
        Some(1_000_000_000),
    ]));
    let int128_col = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(vec![0_u8; 16]),
                None,
                Some(vec![255; 16]),
                Some(vec![128; 16]),
                Some(vec![1; 16]),
            ]
            .into_iter(),
            16,
        )
        .unwrap(),
    );
    let uint128_col = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(vec![0; 16]),
                None,
                Some(vec![255; 16]),
                Some(vec![128; 16]),
                Some(vec![1; 16]),
            ]
            .into_iter(),
            16,
        )
        .unwrap(),
    );
    let int256_col = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(vec![0; 32]),
                None,
                Some(vec![255; 32]),
                Some(vec![128; 32]),
                Some(vec![1; 32]),
            ]
            .into_iter(),
            32,
        )
        .unwrap(),
    );
    let uint256_col = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(vec![0; 32]),
                None,
                Some(vec![255; 32]),
                Some(vec![128; 32]),
                Some(vec![1; 32]),
            ]
            .into_iter(),
            32,
        )
        .unwrap(),
    );
    let float32_col = Arc::new(Float32Array::from(vec![
        Some(0.0_f32),
        None,
        Some(f32::NAN),
        Some(f32::MIN),
        Some(f32::MAX),
    ]));
    let float64_col = Arc::new(Float64Array::from(vec![
        Some(0.0_f64),
        None,
        Some(f64::MIN),
        Some(f64::INFINITY),
        Some(f64::MAX),
    ]));
    // String
    let string_col = Arc::new(StringArray::from(vec![
        Some("hello"),
        None,
        Some(""),
        Some("a".repeat(100).as_str()),
        Some("world"),
    ]));
    let fixed_string_col = Arc::new(StringArray::from(vec![
        Some("fixed12345"),
        None,
        Some("short"),
        Some("toolong12345"),
        Some("exactlyten"),
    ]));
    // Decimal
    let decimal32_col = Arc::new(
        Decimal128Array::from(vec![
            Some(123_456_789),
            None,
            Some(-999_999_999),
            Some(0),
            Some(987_654_321),
        ])
        .with_precision_and_scale(9, 2)
        .unwrap(),
    );
    let decimal64_col = Arc::new(
        Decimal128Array::from(vec![
            Some(123_456_789_012_345_678),
            None,
            Some(-999_999_999_999_999_999),
            Some(0),
            Some(987_654_321_098_765_432),
        ])
        .with_precision_and_scale(18, 4)
        .unwrap(),
    );
    // Datetimes
    let date_col =
        Arc::new(Date32Array::from(vec![Some(0), None, Some(17897), Some(18262), Some(730)]));
    let date32_col = Arc::new(Date32Array::from(vec![
        Some(-149_861), // Jan 3, 1563
        None,
        Some(36524),  // 2000-01-01
        Some(18262),  // 2020-01-01
        Some(-25567), // 1900-01-01
    ]));
    let datetime_col = Arc::new(TimestampMillisecondArray::from(vec![
        Some(0),
        None,
        Some(86_400_000),
        Some(1_577_836_800_000),
        Some(86_400_000),
    ]));
    let datetime_utc_col = Arc::new(
        TimestampMillisecondArray::from_iter(vec![
            Some(0),
            None,
            Some(86_400_000),
            Some(1_577_836_800_000),
            Some(86_400_000),
        ])
        .with_timezone_opt(Some("UTC")),
    );
    let datetime_est_col = Arc::new(
        TimestampMillisecondArray::from_iter(vec![
            Some(0),
            None,
            Some(86_400_000),
            Some(1_577_836_800_000),
            Some(86_400_000),
        ])
        .with_timezone_opt(Some("America/New_York")),
    );
    let datetime64_3_ny_col = Arc::new(
        TimestampMillisecondArray::from(vec![
            Some(0),
            None,
            Some(86_400_000),
            Some(1_577_836_800_000),
            Some(86_400_000),
        ])
        .with_timezone_opt(Some("America/New_York")),
    );
    let datetime64_6_tokyo_col = Arc::new(
        TimestampMicrosecondArray::from(vec![
            Some(0),
            None,
            Some(86_400_000_000),
            Some(1_577_836_800_000_000),
            Some(86_400_000_000),
        ])
        .with_timezone_opt(Some("Asia/Tokyo")),
    );
    let datetime64_9_utc_col = Arc::new(
        TimestampNanosecondArray::from(vec![
            Some(0),
            None,
            Some(86_400_000_000_000),
            Some(1_577_836_800_000_000_000),
            Some(86_400_000_000_000),
        ])
        .with_timezone_opt(Some("UTC")),
    );
    // Map and Tuple
    let fields = Fields::from(vec![
        Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Int32, false)),
        Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Utf8, false)),
    ]);
    let map_array = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new(MAP_FIELD_NAME, DataType::Struct(fields.clone()), false)),
            OffsetBuffer::new(vec![0, 2, 2, 3, 4, 5].into()), // [{1:"a", 2:"b"}, {}, {3:"c"}]
            StructArray::new(
                fields,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
                ],
                None,
            ),
            None,
            false,
        )
        .unwrap(),
    ) as ArrayRef;

    let tuple_int32_string_col = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        ),
        (
            Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
        ),
    ])) as ArrayRef;
    // Special
    let ipv4_col = Arc::new(UInt32Array::from(vec![
        Some(0),
        None,
        Some(3_232_235_521),
        Some(4_294_967_295),
        Some(167_772_161),
    ]));
    let ipv6_col = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(vec![0; 16]),
                None,
                Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
                Some(vec![255; 16]),
                Some(vec![0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            ]
            .into_iter(),
            16,
        )
        .unwrap(),
    );
    let uuid_col = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(vec![0; 16]),
                None,
                Some(vec![255; 16]),
                Some(Uuid::new_v4().as_bytes().to_vec()),
                Some(vec![
                    0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a,
                    0xbc, 0xde, 0xf0,
                ]),
            ]
            .into_iter(),
            16,
        )
        .unwrap(),
    );
    let _json_col = Arc::new(BinaryArray::from_opt_vec(vec![
        Some(b"{\"key\": \"value\"}"),
        None,
        Some(b"[]"),
        Some(
            &format!("{{\"large\": \"{}\"}}", vec!['a'; 100].into_iter().collect::<String>())
                .into_bytes(),
        ),
        Some(b"{\"num\": 42}"),
    ]));
    let _point_col = Arc::new(BinaryArray::from_opt_vec(vec![
        Some(b"\x00\x00\x00\x00\x00\x00\x00\x00"), // [0, 0]
        None,
        Some(b"\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\x00\x40"), /* [1.0, 2.0] */
        Some(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), // [0, 0]
        Some(b"\x00\x00\x00\x00\x00\x00\x14\x40"),                                 // [5.0, 0]
    ]));
    // Enums
    let enum8_col = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(1), Some(0), None]),
            Arc::new(StringArray::from(vec!["active", "inactive"])),
        )
        .unwrap(),
    );
    let enum8_int32_col = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1), Some(0), None]),
            Arc::new(StringArray::from(vec!["active", "inactive"])),
        )
        .unwrap(),
    );
    let enum16_col = Arc::new(
        DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(vec![Some(0), None, Some(1), Some(0), None]),
            Arc::new(StringArray::from(vec!["x", "y"])),
        )
        .unwrap(),
    );
    // LowCardinality
    let low_cardinality_string_col = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter(vec![0, 1, 2, 0, 2]),
            Arc::new(StringArray::from(vec!["low", "card", "test"])),
        )
        .unwrap(),
    );
    let low_cardinality_nullable_string_col = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(3), Some(1), None, Some(2)]),
            Arc::new(StringArray::from(vec!["active", "inactive", "pending", "absent"])),
        )
        .unwrap(),
    );
    // Arrays/Lists
    let large_list_int32_col = Arc::new(
        LargeListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            OffsetBuffer::new(vec![0, 2, 2, 2, 3, 5].into()),
            Arc::new(Int32Array::new(vec![1, 2, i32::MAX, 0, -1].into(), None)),
            None,
        )
        .unwrap(),
    );
    let array_low_cardinality_string_col = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            )),
            OffsetBuffer::new(vec![0, 2, 2, 3, 4, 6].into()),
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![
                        Some(0),
                        Some(1), // Row 1: ["low", "card"]
                        Some(2), // Row 3: ["test"]
                        None,    // Row 4: [null]
                        Some(0),
                        None, // Row 5: ["low", null]
                    ]),
                    Arc::new(StringArray::from(vec!["low", "card", "test"])),
                )
                .unwrap(),
            ),
            None,
        )
        .unwrap(),
    );
    let array_int32_col = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            OffsetBuffer::new(vec![0, 2, 2, 2, 3, 5].into()),
            Arc::new(Int32Array::new(
                vec![
                    1,
                    2,        // Row 1: [1, 2]
                    i32::MAX, // Row 4: [i32::MAX]
                    0,
                    -1, // Row 5: [0, -1]
                ]
                .into(),
                None,
            )),
            None,
        )
        .unwrap(),
    );
    let array_nullable_int32_col = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 3, 4, 5, 6, 7].into()),
            Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(2),        // [1, null, 2]
                None,           // [null]
                Some(0),        // [0]
                None,           // [null]
                Some(i32::MIN), // [i32::MIN]
            ])),
            None, // Non-nullable column
        )
        .unwrap(),
    );
    let array_nullable_string_col = {
        let values = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),                         // Row 1: ["a", null, "b"]
            Some(""),                          // Row 3: [""]
            Some("large".repeat(10).as_str()), // Row 4: ["large..."]
            None,
            Some("x"), // Row 5: [null, "x"]
        ]));
        let offsets = OffsetBuffer::new(vec![0, 3, 3, 4, 5, 7].into());
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        Arc::new(ListArray::try_new(field, offsets, values, None).unwrap())
    };
    let f1 = Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false));
    let f2 = Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false));
    let array_tuple_col = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Arc::clone(&f1), Arc::clone(&f2)].into()),
                false,
            )),
            OffsetBuffer::new(vec![0, 2, 2, 3, 4, 5].into()),
            Arc::new(StructArray::from(vec![
                (f1, Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef),
                (f2, Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef),
            ])),
            None,
        )
        .unwrap(),
    );

    RecordBatch::try_new(test_schema(), vec![
        // Primitives
        id as ArrayRef,
        int8_col,
        int16_col,
        int32_col,
        int64_col,
        uint8_col,
        uint16_col,
        uint32_col,
        uint64_col,
        int128_col,
        uint128_col, // 10
        int256_col,
        uint256_col,
        float32_col,
        float64_col,
        // String
        string_col,
        fixed_string_col,
        // Decimal
        decimal32_col,
        decimal64_col,
        // Datetimes
        date_col,
        date32_col, // 20
        datetime_col,
        datetime_utc_col,
        datetime_est_col,
        datetime64_3_ny_col,
        datetime64_6_tokyo_col,
        datetime64_9_utc_col,
        // Map and Tuple
        map_array,
        tuple_int32_string_col,
        // Special
        ipv4_col,
        ipv6_col, // 30
        uuid_col,
        //
        // TODO
        // json_col,
        // point_col,
        //
        // Enums
        enum8_col,
        enum8_int32_col,
        enum16_col,
        // LowCardinality
        low_cardinality_string_col,
        low_cardinality_nullable_string_col,
        // Arrays
        array_low_cardinality_string_col,
        array_int32_col,
        array_nullable_int32_col,
        array_nullable_string_col, // 40
        large_list_int32_col,
        array_tuple_col,
    ])
    .expect("Failed to create RecordBatch")
}

// --- TESTING LOW CARDINALITY (not currently used, helpful to isolate low card problems) ---

// Low Cardinality
pub fn low_cardinality_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "low_cardinality_string_col",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]))
}

/// # Panics
pub fn low_cardinality_record_batch() -> RecordBatch {
    let low_cardinality_string_col = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter(vec![0, 1, 2, 0, 2]),
            Arc::new(StringArray::from(vec!["low", "card", "test"])),
        )
        .unwrap(),
    );
    RecordBatch::try_new(low_cardinality_schema(), vec![low_cardinality_string_col])
        .expect("Failed to create RecordBatch")
}

// Low Cardinality Nullable
pub fn low_cardinality_nullable_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "low_cardinality_nullable_string_col",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]))
}

/// # Panics
pub fn low_cardinality_nullable_record_batch() -> RecordBatch {
    let low_cardinality_nullable_string_col = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(3), Some(1), None, Some(2)]),
            Arc::new(StringArray::from(vec!["active", "inactive", "pending", "absent"])),
        )
        .unwrap(),
    );

    RecordBatch::try_new(low_cardinality_nullable_schema(), vec![
        low_cardinality_nullable_string_col,
    ])
    .expect("Failed to create RecordBatch")
}

// Low Cardinality Array
pub fn low_cardinality_array_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "array_low_cardinality_string_col",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ))),
        false,
    )]))
}

/// # Panics
pub fn low_cardinality_array_record_batch() -> RecordBatch {
    RecordBatch::try_new(low_cardinality_array_schema(), vec![Arc::new(
        ListArray::try_new(
            Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            )),
            OffsetBuffer::new(vec![0, 2, 2, 3, 4, 6].into()),
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![
                        Some(0),
                        Some(1), // Row 1: ["low", "card"]
                        Some(2), // Row 3: ["test"]
                        None,    // Row 4: [null]
                        Some(0),
                        None, // Row 5: ["low", null]
                    ]),
                    Arc::new(StringArray::from(vec!["low", "card", "test"])),
                )
                .unwrap(),
            ),
            None,
        )
        .unwrap(),
    )])
    .expect("Failed to create RecordBatch")
}
