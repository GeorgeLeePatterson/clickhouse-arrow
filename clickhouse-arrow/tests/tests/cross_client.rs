//! Cross-client primitive round-trip suite.
//!
//! The existing e2e tests round-trip `RecordBatch -> insert -> query -> RecordBatch`
//! entirely through `clickhouse-arrow`. That shape passes even when the crate has
//! symmetric encoding bugs on both sides: read and write cancel each other.
//!
//! This suite breaks the symmetry. For every primitive `ClickHouse` type:
//!
//! 1. The table is created with raw DDL using the `ClickHouse` type name.
//! 2. **Direction A**: rows are inserted via raw SQL literals (so the server parses them). They are
//!    read back via `client.query_rows()` (so the crate's deserializer runs). The returned `Value`s
//!    are asserted against expected.
//! 3. **Direction B**: rows are inserted via an Arrow `RecordBatch` (so the crate's serializer
//!    runs). They are read back via raw SQL using `toString(col)` (so the server formats the
//!    value). The text is asserted against `ClickHouse`'s canonical string form.
//!
//! If any path drops a sign, swaps endianness, mis-shifts an epoch, mangles
//! a precision, or otherwise corrupts a value, one of the two directions will
//! disagree with the server. The fault is localised to read vs. write.

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use clickhouse_arrow::{
    ArrowFormat, ArrowOptions, Client, ClientBuilder, CompressionMethod, DynDateTime64, Qid,
    Result, Value,
};
use futures_util::StreamExt;
use tracing::debug;

use crate::common::header;

type ArrowClient = Client<ArrowFormat>;

// ---------------------------------------------------------------------------
// Bootstrap

async fn connect(ch: &ClickHouseContainer) -> ArrowClient {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    ClientBuilder::default()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_compression(CompressionMethod::default())
        .with_ipv4_only(true)
        .with_arrow_options(
            ArrowOptions::default()
                .with_strings_as_strings(true)
                .with_use_date32_for_date(true)
                .with_strict_schema(false)
                .with_nullable_array_default_empty(true)
                .with_disable_strict_schema_ddl(true),
        )
        .build::<ArrowFormat>()
        .await
        .expect("failed to build client")
}

/// CREATE DATABASE + CREATE TABLE using a single `column_type` definition.
/// Returns `(db, table)` qualified as `db.table` later by the caller.
async fn create_typed_table(client: &ArrowClient, column_type: &str) -> (String, String) {
    let qid = Qid::new();
    let db = format!("xtest_db_{qid}");
    let table = format!("xtest_t_{qid}");

    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid))
        .await
        .expect("create db");
    client
        .execute(
            format!(
                "CREATE TABLE {db}.{table} (id UInt32, v {column_type}) ENGINE = MergeTree ORDER \
                 BY id"
            ),
            Some(qid),
        )
        .await
        .expect("create table");

    (db, table)
}

async fn drop_typed_table(client: &ArrowClient, db: &str, table: &str) {
    let _drop_t = client.execute(format!("DROP TABLE IF EXISTS {db}.{table}"), None).await;
    let _drop_d = client.execute(format!("DROP DATABASE IF EXISTS {db}"), None).await;
}

// ---------------------------------------------------------------------------
// Direction A: SQL-literal INSERT -> arrow read
//
// `rows` is a list of (id, sql_literal, expected_value). The SQL literal is
// pasted into `INSERT INTO t VALUES (id, <literal>)` and parsed server-side;
// the expected_value is what we expect `query_rows` to return for column `v`.

async fn direction_a(client: &ArrowClient, column_type: &str, rows: &[(u32, &str, Value)]) {
    let (db, table) = create_typed_table(client, column_type).await;
    let fq = format!("{db}.{table}");

    // INSERT via raw SQL — every value goes through the CH server's parser.
    let values_list =
        rows.iter().map(|(id, lit, _)| format!("({id}, {lit})")).collect::<Vec<_>>().join(", ");
    client
        .execute(format!("INSERT INTO {fq} VALUES {values_list}"), Some(Qid::new()))
        .await
        .expect("raw insert");

    // SELECT via this crate's deserializer.
    let result: Vec<Vec<Value>> = client
        .query_rows(format!("SELECT id, v FROM {fq} ORDER BY id"), Some(Qid::new()))
        .await
        .expect("query")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("collect");

    assert_eq!(result.len(), rows.len(), "[A:{column_type}] row count");
    let mut mismatches = Vec::new();
    for (got_row, (id, lit, expected)) in result.iter().zip(rows.iter()) {
        // Row layout: [id (UInt32), v]
        assert!(
            matches!(&got_row[0], Value::UInt32(g) if g == id),
            "[A:{column_type}] id mismatch on literal {lit}: got {:?}",
            got_row[0]
        );
        if &got_row[1] != expected {
            mismatches
                .push(format!("literal {lit:?}: got {:?}, expected {expected:?}", got_row[1]));
        }
    }
    assert!(
        mismatches.is_empty(),
        "[A:{column_type}] {} mismatch(es):\n  {}",
        mismatches.len(),
        mismatches.join("\n  ")
    );

    drop_typed_table(client, &db, &table).await;
}

// ---------------------------------------------------------------------------
// Direction B: arrow RecordBatch INSERT -> SQL toString() read
//
// `rows` is a list of (id, ArrayRef-builder-output, expected_text). The Arrow
// array is pushed through `client.insert`; we then ask the server to format
// each value with `toString(v)` and compare the returned bytes against the
// expected canonical text representation.

async fn direction_b(
    client: &ArrowClient,
    column_type: &str,
    arrow_field_type: DataType,
    ids: Vec<u32>,
    values: ArrayRef,
    expected_strings: &[&str],
) {
    assert_eq!(ids.len(), expected_strings.len());
    assert_eq!(values.len(), expected_strings.len());

    let (db, table) = create_typed_table(client, column_type).await;
    let fq = format!("{db}.{table}");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("v", arrow_field_type, true),
    ]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(UInt32Array::from(ids.clone())) as ArrayRef,
        values,
    ])
    .expect("build record batch");

    // INSERT via this crate's serializer.
    let _insert = client
        .insert(format!("INSERT INTO {fq} FORMAT Native"), batch, Some(Qid::new()))
        .await
        .expect("arrow insert")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("collect insert");

    // SELECT toString(v) — the server formats every value to canonical text.
    let result: Vec<Vec<Value>> = client
        .query_rows(format!("SELECT id, toString(v) AS vs FROM {fq} ORDER BY id"), Some(Qid::new()))
        .await
        .expect("query")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("collect");

    assert_eq!(result.len(), expected_strings.len(), "[B:{column_type}] row count");
    // Collect mismatches before asserting so a sweep over many values doesn't
    // stop at the first one — the shape of the disagreement (how many rows,
    // what the offset pattern looks like) is what locates the bug.
    let mut mismatches = Vec::new();
    for (got_row, (id, expected)) in result.iter().zip(ids.iter().zip(expected_strings.iter())) {
        assert!(
            matches!(&got_row[0], Value::UInt32(g) if g == id),
            "[B:{column_type}] id mismatch: got {:?}",
            got_row[0]
        );
        let Value::String(bytes) = &got_row[1] else {
            panic!(
                "[B:{column_type}] expected toString to come back as Value::String, got {:?}",
                got_row[1]
            );
        };
        let got = std::str::from_utf8(bytes).expect("toString utf8");
        if got != *expected {
            mismatches.push(format!("id={id}: got {got:?}, expected {expected:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "[B:{column_type}] {} mismatch(es):\n  {}",
        mismatches.len(),
        mismatches.join("\n  ")
    );

    drop_typed_table(client, &db, &table).await;
}

// ===========================================================================
// The actual test — one function with sections per type group.
// ===========================================================================

/// # Panics
/// Asserts row-by-row equality on every section — any deserializer drift
/// from the server's interpretation aborts the test with the offending
/// literal + decoded value pair. There is no "soft" mode.
#[expect(clippy::too_many_lines)] // type sweep, scope is the value
pub async fn test_cross_client_primitives(ch: Arc<ClickHouseContainer>) {
    let client = connect(ch.as_ref()).await;

    header(Qid::new(), "Section: signed integers");
    direction_a(&client, "Int8", &[
        (0, "-128", Value::Int8(i8::MIN)),
        (1, "0", Value::Int8(0)),
        (2, "127", Value::Int8(i8::MAX)),
    ])
    .await;
    direction_b(
        &client,
        "Int8",
        DataType::Int8,
        vec![0, 1, 2],
        Arc::new(Int8Array::from(vec![i8::MIN, 0, i8::MAX])) as ArrayRef,
        &["-128", "0", "127"],
    )
    .await;

    direction_a(&client, "Int16", &[
        (0, "-32768", Value::Int16(i16::MIN)),
        (1, "0", Value::Int16(0)),
        (2, "32767", Value::Int16(i16::MAX)),
    ])
    .await;
    direction_b(
        &client,
        "Int16",
        DataType::Int16,
        vec![0, 1, 2],
        Arc::new(Int16Array::from(vec![i16::MIN, 0, i16::MAX])) as ArrayRef,
        &["-32768", "0", "32767"],
    )
    .await;

    direction_a(&client, "Int32", &[
        (0, "-2147483648", Value::Int32(i32::MIN)),
        (1, "0", Value::Int32(0)),
        (2, "2147483647", Value::Int32(i32::MAX)),
    ])
    .await;
    direction_b(
        &client,
        "Int32",
        DataType::Int32,
        vec![0, 1, 2],
        Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])) as ArrayRef,
        &["-2147483648", "0", "2147483647"],
    )
    .await;

    direction_a(&client, "Int64", &[
        (0, "-9223372036854775808", Value::Int64(i64::MIN)),
        (1, "0", Value::Int64(0)),
        (2, "9223372036854775807", Value::Int64(i64::MAX)),
    ])
    .await;
    direction_b(
        &client,
        "Int64",
        DataType::Int64,
        vec![0, 1, 2],
        Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])) as ArrayRef,
        &["-9223372036854775808", "0", "9223372036854775807"],
    )
    .await;

    header(Qid::new(), "Section: unsigned integers");
    direction_a(&client, "UInt8", &[(0, "0", Value::UInt8(0)), (1, "255", Value::UInt8(u8::MAX))])
        .await;
    direction_b(
        &client,
        "UInt8",
        DataType::UInt8,
        vec![0, 1],
        Arc::new(UInt8Array::from(vec![0_u8, u8::MAX])) as ArrayRef,
        &["0", "255"],
    )
    .await;

    direction_a(&client, "UInt16", &[
        (0, "0", Value::UInt16(0)),
        (1, "65535", Value::UInt16(u16::MAX)),
    ])
    .await;
    direction_b(
        &client,
        "UInt16",
        DataType::UInt16,
        vec![0, 1],
        Arc::new(UInt16Array::from(vec![0_u16, u16::MAX])) as ArrayRef,
        &["0", "65535"],
    )
    .await;

    direction_a(&client, "UInt32", &[
        (0, "0", Value::UInt32(0)),
        (1, "4294967295", Value::UInt32(u32::MAX)),
    ])
    .await;
    direction_b(
        &client,
        "UInt32",
        DataType::UInt32,
        vec![0, 1],
        Arc::new(UInt32Array::from(vec![0_u32, u32::MAX])) as ArrayRef,
        &["0", "4294967295"],
    )
    .await;

    direction_a(&client, "UInt64", &[
        (0, "0", Value::UInt64(0)),
        (1, "18446744073709551615", Value::UInt64(u64::MAX)),
    ])
    .await;
    direction_b(
        &client,
        "UInt64",
        DataType::UInt64,
        vec![0, 1],
        Arc::new(UInt64Array::from(vec![0_u64, u64::MAX])) as ArrayRef,
        &["0", "18446744073709551615"],
    )
    .await;

    header(Qid::new(), "Section: floats");
    // NaN's bit pattern is preserved (PartialEq on Value::Float uses to_bits).
    // toString() formats NaN as "nan", inf as "inf", subnormals as decimal text.
    direction_a(&client, "Float32", &[
        (0, "0", Value::Float32(0.0)),
        (1, "-0", Value::Float32(-0.0)),
        (2, "1.5", Value::Float32(1.5)),
        (3, "-1.5", Value::Float32(-1.5)),
        (4, "inf", Value::Float32(f32::INFINITY)),
        (5, "-inf", Value::Float32(f32::NEG_INFINITY)),
    ])
    .await;
    direction_b(
        &client,
        "Float32",
        DataType::Float32,
        vec![0, 1, 2, 3, 4, 5],
        Arc::new(Float32Array::from(vec![
            0.0_f32,
            -0.0_f32,
            1.5_f32,
            -1.5_f32,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ])) as ArrayRef,
        &["0", "-0", "1.5", "-1.5", "inf", "-inf"],
    )
    .await;

    direction_a(&client, "Float64", &[
        (0, "0", Value::Float64(0.0)),
        (1, "-0", Value::Float64(-0.0)),
        (2, "1.5", Value::Float64(1.5)),
        (3, "-1.5", Value::Float64(-1.5)),
        (4, "inf", Value::Float64(f64::INFINITY)),
        (5, "-inf", Value::Float64(f64::NEG_INFINITY)),
    ])
    .await;
    direction_b(
        &client,
        "Float64",
        DataType::Float64,
        vec![0, 1, 2, 3, 4, 5],
        Arc::new(Float64Array::from(vec![
            0.0_f64,
            -0.0_f64,
            1.5_f64,
            -1.5_f64,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ])) as ArrayRef,
        &["0", "-0", "1.5", "-1.5", "inf", "-inf"],
    )
    .await;

    header(Qid::new(), "Section: Date (UInt16 days-since-1970)");
    // Date range: 1970-01-01..=2149-06-06 (the post-2106 extension landed in
    // CH 22.x). 65535 ~= 2149-06-06.
    direction_a(&client, "Date", &[
        (0, "'1970-01-01'", Value::Date(Date(0))),
        (1, "'2024-12-31'", Value::Date(Date(20088))),
    ])
    .await;
    direction_b(
        &client,
        "Date",
        DataType::Date32,
        vec![0, 1],
        Arc::new(Date32Array::from(vec![0, 20088])) as ArrayRef,
        &["1970-01-01", "2024-12-31"],
    )
    .await;

    header(Qid::new(), "Section: Date32 (Int32 days-since-1970)");
    // Probes the full Date32 range, including pre-1970 and post-2149 values
    // that don't fit a plain Date.
    direction_a(&client, "Date32", &[
        (0, "'1900-01-01'", Value::Date32(Date32(-25567))),
        (1, "'1969-12-31'", Value::Date32(Date32(-1))),
        (2, "'1970-01-01'", Value::Date32(Date32(0))),
        (3, "'2024-12-31'", Value::Date32(Date32(20088))),
        (4, "'2106-02-07'", Value::Date32(Date32(49710))),
    ])
    .await;
    direction_b(
        &client,
        "Date32",
        DataType::Date32,
        vec![0, 1, 2, 3, 4, 5],
        Arc::new(Date32Array::from(vec![
            -25567_i32, // 1900-01-01 (Date32-only range)
            -1,         // 1969-12-31 (Date32-only range)
            0,          // 1970-01-01
            20088,      // 2024-12-31
            49702,      // 2106-01-30 (past plain-Date range)
            65535,      // 2149-06-06 (u16::MAX boundary)
        ])) as ArrayRef,
        &["1900-01-01", "1969-12-31", "1970-01-01", "2024-12-31", "2106-01-30", "2149-06-06"],
    )
    .await;

    header(Qid::new(), "Section: DateTime (UInt32 seconds-since-epoch UTC)");
    direction_a(&client, "DateTime('UTC')", &[
        (0, "'1970-01-01 00:00:00'", Value::DateTime(DateTime(chrono_tz::UTC, 0))),
        (1, "'2024-12-31 23:59:59'", Value::DateTime(DateTime(chrono_tz::UTC, 1_735_689_599))),
    ])
    .await;
    direction_b(
        &client,
        "DateTime('UTC')",
        DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
        vec![0, 1],
        Arc::new(
            TimestampSecondArray::from(vec![0_i64, 1_735_689_599]).with_timezone(Arc::from("UTC")),
        ) as ArrayRef,
        &["1970-01-01 00:00:00", "2024-12-31 23:59:59"],
    )
    .await;

    header(Qid::new(), "Section: DateTime64 — subsecond precision sweep");
    direction_a(&client, "DateTime64(3, 'UTC')", &[
        (0, "'1970-01-01 00:00:00.000'", Value::DateTime64(DynDateTime64(chrono_tz::UTC, 0, 3))),
        (
            1,
            "'2024-12-31 23:59:59.999'",
            Value::DateTime64(DynDateTime64(chrono_tz::UTC, 1_735_689_599_999, 3)),
        ),
    ])
    .await;
    direction_a(&client, "DateTime64(6, 'UTC')", &[(
        0,
        "'2024-06-15 12:00:00.123456'",
        Value::DateTime64(DynDateTime64(chrono_tz::UTC, 1_718_452_800_123_456, 6)),
    )])
    .await;
    direction_a(&client, "DateTime64(9, 'UTC')", &[(
        0,
        "'2024-06-15 12:00:00.123456789'",
        Value::DateTime64(DynDateTime64(chrono_tz::UTC, 1_718_452_800_123_456_789, 9)),
    )])
    .await;

    header(Qid::new(), "Section: String — UTF-8, empty, embedded NUL");
    direction_a(&client, "String", &[
        (0, "''", Value::String(b"".to_vec())),
        (1, "'hello'", Value::String(b"hello".to_vec())),
        (2, "'café'", Value::String("café".as_bytes().to_vec())),
        (3, "'a\\0b'", Value::String(b"a\0b".to_vec())),
    ])
    .await;
    direction_b(
        &client,
        "String",
        DataType::Utf8,
        vec![0, 1, 2, 3],
        Arc::new(StringArray::from(vec!["", "hello", "café", "a\0b"])) as ArrayRef,
        &["", "hello", "café", "a\0b"],
    )
    .await;

    header(Qid::new(), "Section: UUID");
    // CH writes UUIDs as a 128-bit value laid out as two LE u64s in
    // (high, low) order. The `binary_async!(Uuid)` arm reverses each
    // 8-byte half so bytes land in canonical RFC 4122 order before
    // Uuid::from_bytes reads them.
    direction_a(&client, "UUID", &[
        (0, "'00000000-0000-0000-0000-000000000000'", Value::Uuid(uuid::Uuid::nil())),
        (
            1,
            "'12345678-1234-5678-1234-567812345678'",
            Value::Uuid(uuid::Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap()),
        ),
        (
            2,
            "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
            Value::Uuid(uuid::Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap()),
        ),
    ])
    .await;

    header(Qid::new(), "Section: Bool");
    direction_a(&client, "Bool", &[(0, "false", Value::UInt8(0)), (1, "true", Value::UInt8(1))])
        .await;
    direction_b(
        &client,
        "Bool",
        DataType::Boolean,
        vec![0, 1],
        Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef,
        &["false", "true"],
    )
    .await;

    header(Qid::new(), "Section: wide integers (Int128, UInt128)");
    direction_a(&client, "Int128", &[
        (0, "-170141183460469231731687303715884105728", Value::Int128(i128::MIN)),
        (1, "0", Value::Int128(0)),
        (2, "170141183460469231731687303715884105727", Value::Int128(i128::MAX)),
    ])
    .await;
    direction_a(&client, "UInt128", &[
        (0, "0", Value::UInt128(0)),
        (1, "340282366920938463463374607431768211455", Value::UInt128(u128::MAX)),
    ])
    .await;

    header(Qid::new(), "Section: Decimal32/64/128 — narrow + wide precision");
    direction_a(&client, "Decimal32(2)", &[
        (0, "0", Value::Decimal32(2, 0)),
        (1, "12345.67", Value::Decimal32(2, 1_234_567)),
        (2, "-12345.67", Value::Decimal32(2, -1_234_567)),
    ])
    .await;
    direction_a(&client, "Decimal64(4)", &[
        (0, "0", Value::Decimal64(4, 0)),
        (1, "12345.6789", Value::Decimal64(4, 123_456_789)),
        (2, "-12345.6789", Value::Decimal64(4, -123_456_789)),
    ])
    .await;
    direction_a(&client, "Decimal128(6)", &[
        (0, "0", Value::Decimal128(6, 0)),
        (1, "123456789012.345678", Value::Decimal128(6, 123_456_789_012_345_678_i128)),
        (2, "-123456789012.345678", Value::Decimal128(6, -123_456_789_012_345_678_i128)),
    ])
    .await;

    header(Qid::new(), "Section: IPv4");
    direction_a(&client, "IPv4", &[
        (0, "'127.0.0.1'", Value::Ipv4(Ipv4(std::net::Ipv4Addr::LOCALHOST))),
        (1, "'192.168.1.42'", Value::Ipv4(Ipv4(std::net::Ipv4Addr::new(192, 168, 1, 42)))),
    ])
    .await;

    client.shutdown().await.expect("shutdown");
}

// ===========================================================================
// Sparse default-fill correctness.
//
// When a non-nullable column is mostly equal to its type default, CH writes
// the part with Sparse serialization: it ships only the non-default values
// plus an offset stream, and the reader must reconstruct every omitted row as
// the type's *zero* value.
//
// Sparse reconstruction must build omitted non-nullable rows as type-correct
// zero/default values. Decimal and DateTime64 are included because they exposed
// prior default-fill mistakes that small dense rows did not hit.
//
// These tests force sparse serialization with
// `ratio_of_defaults_for_sparse_serialization = 0.0` (every column whose ratio
// of defaults is >= 0 is eligible — i.e. always), read back through Arrow, and
// assert the default rows are the type zero with `null_count == 0`.
// ===========================================================================

/// Create a `MergeTree` table that forces sparse serialization for any column
/// dominated by defaults, returning `(db, table)`.
async fn create_sparse_forcing_table(client: &ArrowClient, column_type: &str) -> (String, String) {
    let qid = Qid::new();
    let db = format!("xsparse_db_{qid}");
    let table = format!("xsparse_t_{qid}");

    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid))
        .await
        .expect("create db");
    client
        .execute(
            format!(
                "CREATE TABLE {db}.{table} (id UInt32, v {column_type}) ENGINE = MergeTree ORDER \
                 BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0"
            ),
            Some(qid),
        )
        .await
        .expect("create table");

    (db, table)
}

/// Read column `v` back as a single materialized Arrow column.
///
/// Concatenates every streamed batch so the assertion sees the whole column
/// at once (`null_count` is meaningful only over the full column).
async fn read_v_column(client: &ArrowClient, fq: &str) -> ArrayRef {
    let batches: Vec<RecordBatch> = client
        .query(format!("SELECT id, v FROM {fq} ORDER BY id"), Some(Qid::new()))
        .await
        .expect("query")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("collect batches");

    assert!(!batches.is_empty(), "no batches returned for {fq}");
    let v_arrays: Vec<&dyn Array> =
        batches.iter().map(|b| b.column_by_name("v").expect("column v").as_ref()).collect();
    arrow::compute::concat(&v_arrays).expect("concat v column")
}

/// Build a mostly-default column body: `total` rows, all the SQL default
/// literal except a few sentinel rows. Returns the `(id, literal)` insert
/// pairs plus the set of non-default ids for the caller to assert against.
fn mostly_default_rows<'a>(
    total: u32,
    default_lit: &'a str,
    sentinels: &'a [(u32, &'a str)],
) -> Vec<(u32, String)> {
    (0..total)
        .map(|id| {
            let lit =
                sentinels.iter().find(|(sid, _)| *sid == id).map_or(default_lit, |(_, lit)| lit);
            (id, lit.to_string())
        })
        .collect()
}

/// Insert `(id, literal)` rows via raw SQL into `fq`.
async fn sql_insert(client: &ArrowClient, fq: &str, rows: &[(u32, String)]) {
    let values_list =
        rows.iter().map(|(id, lit)| format!("({id}, {lit})")).collect::<Vec<_>>().join(", ");
    client
        .execute(format!("INSERT INTO {fq} VALUES {values_list}"), Some(Qid::new()))
        .await
        .expect("raw insert");
}

/// # Panics
/// Asserts the sparse-default rows of non-nullable `Decimal` and `DateTime64`
/// columns come back as the type zero, not NULL.
pub async fn test_sparse_default_fill_non_nullable(ch: Arc<ClickHouseContainer>) {
    const TOTAL: u32 = 256;

    let client = connect(ch.as_ref()).await;

    // --- Decimal64(4): defaults must materialize as 0, not NULL -----------
    header(Qid::new(), "Sparse: non-nullable Decimal64(4) default-fill");
    {
        let (db, table) = create_sparse_forcing_table(&client, "Decimal64(4)").await;
        let fq = format!("{db}.{table}");
        // Three non-default sentinels; everything else is 0.
        let sentinels: &[(u32, &str)] = &[(10, "12.3456"), (100, "-7.8900"), (200, "0.0001")];
        let rows = mostly_default_rows(TOTAL, "0", sentinels);
        sql_insert(&client, &fq, &rows).await;

        let col = read_v_column(&client, &fq).await;
        assert_eq!(col.len(), TOTAL as usize, "[sparse Decimal64] row count");
        assert_eq!(
            col.null_count(),
            0,
            "[sparse Decimal64] non-nullable column must have no nulls; sparse default rows came \
             back NULL"
        );
        let dec = col.as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");
        // A default (id=0) row: must be raw mantissa 0, not null.
        assert!(!dec.is_null(0), "[sparse Decimal64] default row 0 is NULL");
        assert_eq!(dec.value(0), 0_i128, "[sparse Decimal64] default row 0 mantissa");
        // The sentinel at id=10 = 12.3456 with scale 4 -> mantissa 123456.
        assert!(!dec.is_null(10), "[sparse Decimal64] sentinel row 10 is NULL");
        assert_eq!(dec.value(10), 123_456_i128, "[sparse Decimal64] sentinel row 10 mantissa");

        drop_typed_table(&client, &db, &table).await;
    }

    // --- DateTime64(3, 'UTC'): defaults must materialize as epoch, not NULL
    header(Qid::new(), "Sparse: non-nullable DateTime64(3) default-fill");
    {
        let (db, table) = create_sparse_forcing_table(&client, "DateTime64(3, 'UTC')").await;
        let fq = format!("{db}.{table}");
        let sentinels: &[(u32, &str)] =
            &[(10, "'2024-12-31 23:59:59.999'"), (200, "'2024-06-15 12:00:00.123'")];
        // The DateTime64 default is the epoch '1970-01-01 00:00:00.000'.
        let rows = mostly_default_rows(TOTAL, "'1970-01-01 00:00:00.000'", sentinels);
        sql_insert(&client, &fq, &rows).await;

        let col = read_v_column(&client, &fq).await;
        assert_eq!(col.len(), TOTAL as usize, "[sparse DateTime64] row count");
        assert_eq!(
            col.null_count(),
            0,
            "[sparse DateTime64] non-nullable column must have no nulls; sparse default rows came \
             back NULL"
        );
        let ts = col
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("TimestampMillisecondArray");
        // Default (id=0) row: epoch == 0 ms, not null.
        assert!(!ts.is_null(0), "[sparse DateTime64] default row 0 is NULL");
        assert_eq!(ts.value(0), 0_i64, "[sparse DateTime64] default row 0 epoch ms");
        // Sentinel id=10 = 2024-12-31 23:59:59.999 UTC -> 1_735_689_599_999 ms.
        assert!(!ts.is_null(10), "[sparse DateTime64] sentinel row 10 is NULL");
        assert_eq!(ts.value(10), 1_735_689_599_999_i64, "[sparse DateTime64] sentinel row 10 ms");

        drop_typed_table(&client, &db, &table).await;
    }

    // --- Nullable(Decimal64(4)): the Nullable default IS null, so the
    // omitted rows must come back NULL (the `is_nullable` branch of
    // default_array_of), while the sentinels carry values. -----------------
    header(Qid::new(), "Sparse: Nullable(Decimal64(4)) default-fill is NULL");
    {
        let (db, table) = create_sparse_forcing_table(&client, "Nullable(Decimal64(4))").await;
        let fq = format!("{db}.{table}");
        // Mostly NULL; a couple of non-null sentinels. NULL dominates so CH
        // still serializes the column sparse.
        let sentinels: &[(u32, &str)] = &[(10, "12.3456"), (200, "-7.8900")];
        let rows = mostly_default_rows(TOTAL, "NULL", sentinels);
        sql_insert(&client, &fq, &rows).await;

        let col = read_v_column(&client, &fq).await;
        assert_eq!(col.len(), TOTAL as usize, "[sparse Nullable Decimal64] row count");
        // Every default (omitted) row must be NULL; only the two sentinels
        // are non-null.
        assert_eq!(
            col.null_count(),
            TOTAL as usize - sentinels.len(),
            "[sparse Nullable Decimal64] omitted rows must materialize as NULL"
        );
        let dec = col.as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");
        assert!(dec.is_null(0), "[sparse Nullable Decimal64] default row 0 should be NULL");
        assert!(!dec.is_null(10), "[sparse Nullable Decimal64] sentinel row 10 is NULL");
        assert_eq!(dec.value(10), 123_456_i128, "[sparse Nullable Decimal64] sentinel row 10");

        drop_typed_table(&client, &db, &table).await;
    }

    client.shutdown().await.expect("shutdown");
}
