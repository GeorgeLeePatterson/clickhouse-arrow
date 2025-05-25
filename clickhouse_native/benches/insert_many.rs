#![expect(unused_crate_dependencies)]
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse::{Client as ClickhouseRsClient, Compression, Row as ClickhouseRow};
use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::{
    ClickHouseContainer, arrow_tests, get_or_create_container, init_tracing,
};
use clickhouse_native::{CompressionMethod, Row, Uuid};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::{StreamExt, stream};
use klickhouse::{
    Client as KlickhouseClient, ClientOptions as KlientOptions, DateTime64 as KDateTime64,
    Row as KRow,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

const TEST_DB_NAME: &str = "benchmark_test";

#[derive(ClickhouseRow, Clone, Serialize, Deserialize)]
struct ClickhouseRsRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    i64, // DateTime64(3) maps to i64 milliseconds
}

#[derive(Row, Clone, Serialize, Deserialize)]
struct ClickhouseNativeRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    DateTime64<3>,
}

#[derive(KRow, Clone)]
struct KlickhouseRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    KDateTime64<3>, // DateTime64(3) maps to i64 milliseconds
}

#[allow(unused)]
async fn setup_clickhouse_native(ch: &'static ClickHouseContainer) -> Result<NativeClient> {
    Client::<NativeFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        // .with_compression(CompressionMethod::LZ4)
        .with_compression(CompressionMethod::None)
        .build()
        .await
}

fn setup_clickhouse_rs(ch: &'static ClickHouseContainer) -> ClickhouseRsClient {
    ClickhouseRsClient::default()
        .with_url(ch.get_http_url())
        .with_user(&ch.user)
        .with_password(&ch.password)
        .with_database("default")
        .with_compression(Compression::None)
    // .with_compression(Compression::Lz4)
}

#[allow(unused)]
async fn setup_klickhouse(ch: &'static ClickHouseContainer) -> Result<KlickhouseClient> {
    KlickhouseClient::connect(ch.get_native_url(), KlientOptions {
        username:         ch.user.to_string(),
        password:         ch.password.to_string(),
        default_database: "default".to_string(),
        tcp_nodelay:      true,
    })
    .await
    .map_err(|e| Error::External(Box::new(e)))
}

// Helper function to create test rows for clickhouse-rs
#[expect(clippy::cast_precision_loss)]
#[expect(clippy::cast_possible_wrap)]
fn create_test_rows(rows: usize) -> Vec<ClickhouseRsRow> {
    (0..rows)
        .map(|i| ClickhouseRsRow {
            id:    Uuid::new_v4().to_string(),
            name:  format!("name{i}"),
            value: i as f64,
            ts:    i as i64 * 1000,
        })
        .collect()
}

// Helper function to create test rows for clickhouse-rs
#[allow(unused)]
#[expect(clippy::cast_precision_loss)]
#[expect(clippy::cast_possible_wrap)]
fn create_test_native_rows(rows: usize) -> Vec<ClickhouseNativeRow> {
    (0..rows)
        .map(|i| ClickhouseNativeRow {
            id:    Uuid::new_v4().to_string(),
            name:  format!("name{i}"),
            value: i as f64,
            ts:    DateTime64::<3>::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(i as i64 * 1000, 0).unwrap(),
            )
            .unwrap(),
        })
        .collect()
}

// Helper function to create test rows for clickhouse-rs
#[expect(clippy::cast_precision_loss)]
#[expect(clippy::cast_possible_wrap)]
fn create_test_klickhouse_rows(rows: usize) -> Vec<KlickhouseRow> {
    (0..rows)
        .map(|i| KlickhouseRow {
            id:    Uuid::new_v4().to_string(),
            name:  format!("name{i}"),
            value: i as f64,
            ts:    KDateTime64::<3>::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(i as i64 * 1000, 0).unwrap(),
            )
            .unwrap(),
        })
        .collect()
}

fn init() {
    if let Ok(l) = std::env::var("RUST_LOG") {
        if !l.is_empty() {
            // Add directives here
            init_tracing(Some(&[/*("tokio", "error")*/]));
        }
    }
}

fn print_msg(msg: impl std::fmt::Display) {
    eprintln!("\n--------\n{msg}\n--------\n\n");
}

fn insert_arrow(
    table: &str,
    rows: usize,
    client: &ArrowClient,
    batches: &[RecordBatch],
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    // Benchmark native arrow insert
    let query = format!("INSERT INTO {table} FORMAT NATIVE");
    let _ = group
        // Reduce sample size for slower operations
        .sample_size(50)
        .measurement_time(Duration::from_secs(10))
        .bench_with_input(
            BenchmarkId::new("clickhouse_arrow", rows),
            &(&query, client),
            |b, (query, client)| {
                b.to_async(rt).iter_batched(
                    || batches.to_vec(),
                    |batches| async move {
                        let stream = client
                            .insert_many(query.as_str(), batches, None)
                            .await
                            .inspect_err(|e| print_msg(format!("Insert error\n{e:?}")))
                            .unwrap();
                        drop(black_box(stream));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
}

fn insert_rs(
    table: &str,
    rows: usize,
    client: &ClickhouseRsClient,
    batch: &[ClickhouseRsRow],
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let _ = group.sample_size(50).measurement_time(Duration::from_secs(10)).bench_with_input(
        BenchmarkId::new("rs_rowbinary", rows),
        &(table, client),
        |b, (table, client)| {
            b.to_async(rt).iter_batched(
                || batch.to_vec(), // Setup: clone the rows for each iteration
                |rows| async move {
                    let mut insert = client.insert(table).unwrap();
                    for row in rows {
                        insert.write(&row).await.unwrap();
                    }
                    insert.end().await.unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );
}

fn insert_kh(
    table: &str,
    rows: usize,
    client: &KlickhouseClient,
    batch: &[KlickhouseRow],
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let query = format!("INSERT INTO {table} FORMAT NATIVE");
    let _ = group
        // Reduce sample size for slower operations
        .sample_size(50)
        .measurement_time(Duration::from_secs(10))
        .bench_with_input(
            BenchmarkId::new("klickhouse", rows),
            &(&query, client),
            |b, (query, client)| {
                b.to_async(rt).iter_batched(
                    || batch.to_vec(), // Setup: clone the rows for each iteration
                    |rows| async move {
                        client
                            .insert_native(
                                query.as_str(),
                                Box::pin(stream::once(async move { rows })),
                            )
                            .await
                            .unwrap();
                        black_box(());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
}

fn query_arrow(
    table: &str,
    rows: usize,
    client: &ArrowClient,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let query = format!("SELECT * FROM {table} LIMIT {rows}");
    let _ = group.bench_with_input(
        BenchmarkId::new("clickhouse_arrow", rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let mut stream = client
                    .query(query.as_str(), None)
                    .await
                    .inspect_err(|e| print_msg(format!("Query error: {e:?}")))
                    .unwrap();
                let mut batches = Vec::with_capacity(rows);
                while let Some(b) = stream.next().await {
                    batches
                        .push(b.inspect_err(|e| print_msg(format!("Batch error: {e:?}"))).unwrap());
                }
                black_box(batches)
            });
        },
    );
}

fn query_rs(
    table: &str,
    rows: usize,
    client: &ClickhouseRsClient,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let query = format!("SELECT * FROM {table} LIMIT {rows}");
    let _ = group.bench_with_input(
        BenchmarkId::new("rs_rowbinary", rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let result = client.query(query).fetch_all::<ClickhouseRsRow>().await.unwrap();
                black_box(result)
            });
        },
    );
}

fn query_kh(
    table: &str,
    rows: usize,
    client: &KlickhouseClient,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let query = format!("SELECT * FROM {table} LIMIT {rows}");
    let _ = group.bench_with_input(
        BenchmarkId::new("klickhouse", rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let mut result = client.query::<KlickhouseRow>(query).await.unwrap();
                let mut rows = Vec::with_capacity(rows);
                while let Some(row) = result.next().await {
                    rows.push(row.unwrap());
                }
                black_box(rows)
            });
        },
    );
}

#[allow(clippy::too_many_lines)]
fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Init tracing
    init();

    // Setup container once
    let ch = rt.block_on(get_or_create_container());
    print_msg("Created container");

    // Test with different row counts
    // let row_counts = vec![100, 1000, 10_000, 100_000];
    // let row_counts = vec![100_000, 100_000, 100_000, 100_000];
    let row_counts = vec![100_000, 200_000, 300_000, 400_000];
    let batch_size = 4;

    for rows in row_counts {
        print_msg(format!("Running test for {rows} rows"));

        // Pre-create the batch and rows to avoid including this in benchmark time
        let batches: Vec<RecordBatch> = (0..batch_size)
            .map(|_| arrow_tests::create_test_batch(rows / batch_size, false))
            .collect::<Vec<_>>();
        let test_rows = create_test_rows(rows);
        let test_kh_rows = create_test_klickhouse_rows(rows);
        let schema = batches[0].schema();

        // Setup clients
        let arrow_client_builder =
            arrow_tests::setup_test_arrow_client(ch).with_compression(CompressionMethod::None);
        let arrow_client = rt
            .block_on(arrow_client_builder.build::<ArrowFormat>())
            .expect("clickhouse native arrow setup");
        let rs_client = setup_clickhouse_rs(ch);
        let kh_client = rt.block_on(setup_klickhouse(ch)).expect("klickhouse client setup");

        // Setup database
        rt.block_on(arrow_tests::setup_database(TEST_DB_NAME, &arrow_client))
            .expect("setup database");

        // Setup tables
        let arrow_table_ref = rt
            .block_on(arrow_tests::setup_table(&arrow_client, TEST_DB_NAME, &schema))
            .expect("clickhouse rs table");
        let rs_table_ref = rt
            .block_on(arrow_tests::setup_table(&arrow_client, TEST_DB_NAME, &schema))
            .expect("clickhouse rs table");
        let kh_table_ref = rt
            .block_on(arrow_tests::setup_table(&arrow_client, TEST_DB_NAME, &schema))
            .expect("klickhouse table");

        // Wrap clients in Arc for sharing across iterations
        let arrow_client = Arc::new(arrow_client);
        let rs_client = Arc::new(rs_client);
        let kh_client = Arc::new(kh_client);

        let mut insert_group = c.benchmark_group(format!("insert_many_{rows}_rows"));

        // Benchmark native arrow insert
        insert_arrow(
            &arrow_table_ref,
            rows,
            arrow_client.as_ref(),
            &batches,
            &mut insert_group,
            &rt,
        );

        // Benchmark clickhouse-rs insert
        insert_rs(&rs_table_ref, rows, rs_client.as_ref(), &test_rows, &mut insert_group, &rt);

        // Benchmark klickhouse insert
        insert_kh(&kh_table_ref, rows, kh_client.as_ref(), &test_kh_rows, &mut insert_group, &rt);

        insert_group.finish();

        // Query benchmarks
        let mut query_group = c.benchmark_group(format!("query_{rows}_rows"));

        // Benchmark native arrow query
        query_arrow(&arrow_table_ref, rows, arrow_client.as_ref(), &mut query_group, &rt);

        // Benchmark clickhouse-rs query
        query_rs(&rs_table_ref, rows, rs_client.as_ref(), &mut query_group, &rt);

        // Benchmark klickhouse query
        query_kh(&kh_table_ref, rows, kh_client.as_ref(), &mut query_group, &rt);

        query_group.finish();
    }

    // Shutdown container
    rt.block_on(ch.shutdown()).expect("Shutting down container");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
