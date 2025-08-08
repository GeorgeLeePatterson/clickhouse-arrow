#![expect(unused_crate_dependencies)]
mod common;

use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::CompressionMethod;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{arrow_tests, get_or_create_container};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::StreamExt;
use tokio::runtime::Runtime;

use self::common::{init, print_msg};

fn insert_arrow(
    compression: CompressionMethod,
    table: &str,
    rows: usize,
    client: &ArrowClient,
    batch: &RecordBatch,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    // Benchmark native arrow insert
    let query = format!("INSERT INTO {table} FORMAT NATIVE");
    let _ = group.sample_size(50).measurement_time(Duration::from_secs(10)).bench_with_input(
        BenchmarkId::new(format!("clickhouse_arrow_{compression}"), rows),
        &(&query, client),
        |b, (query, client)| {
            b.to_async(rt).iter_batched(
                || batch.clone(),
                |batch| async move {
                    let stream = client
                        .insert(query.as_str(), batch, None)
                        .await
                        .inspect_err(|e| print_msg(format!("Insert error\n{e:?}")))
                        .unwrap();
                    drop(stream);
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );
}

fn query_arrow(
    compression: CompressionMethod,
    table: &str,
    rows: usize,
    client: &ArrowClient,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let query = format!("SELECT * FROM {table} LIMIT {rows}");
    let _ = group.bench_with_input(
        BenchmarkId::new(format!("clickhouse_arrow_{compression}"), rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let mut stream = client
                    .query(query.as_str(), None)
                    .await
                    .inspect_err(|e| print_msg(format!("Query error: {e:?}")))
                    .unwrap();
                while let Some(result) = stream.next().await {
                    drop(result.unwrap());
                }
            });
        },
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Init tracing
    init();

    // Setup container once
    let ch = rt.block_on(get_or_create_container(None));
    print_msg("Created container");

    // Test with different row counts
    let row_counts = vec![10_000, 100_000, 200_000, 300_000, 400_000];

    for rows in row_counts {
        print_msg(format!("Running test for {rows} rows"));

        // Pre-create the batch and rows to avoid including this in benchmark time
        let batch = arrow_tests::create_test_batch(rows, false);
        let schema = batch.schema();

        // Setup clients
        let client_builder =
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true);

        let builder = client_builder.clone().with_compression(CompressionMethod::None);
        let arrow_client_none =
            rt.block_on(builder.build::<ArrowFormat>()).expect("clickhouse native arrow setup");

        let builder = client_builder.clone().with_compression(CompressionMethod::LZ4); // Default
        let arrow_client_lz4 =
            rt.block_on(builder.build::<ArrowFormat>()).expect("clickhouse native arrow setup");

        let builder = client_builder.clone().with_compression(CompressionMethod::ZSTD);
        let arrow_client_zstd =
            rt.block_on(builder.build::<ArrowFormat>()).expect("clickhouse native arrow setup");

        // Setup database
        rt.block_on(arrow_tests::setup_database(common::TEST_DB_NAME, &arrow_client_lz4))
            .expect("setup database");

        // Setup tables
        let arrow_table_ref = rt
            .block_on(arrow_tests::setup_table(&arrow_client_lz4, common::TEST_DB_NAME, &schema))
            .expect("clickhouse table");

        // Benchmark native arrow inserts
        let mut insert_group = c.benchmark_group("InsertCompression");

        insert_arrow(
            CompressionMethod::None,
            &arrow_table_ref,
            rows,
            &arrow_client_none,
            &batch,
            &mut insert_group,
            &rt,
        );

        insert_arrow(
            CompressionMethod::LZ4,
            &arrow_table_ref,
            rows,
            &arrow_client_lz4,
            &batch,
            &mut insert_group,
            &rt,
        );

        insert_arrow(
            CompressionMethod::ZSTD,
            &arrow_table_ref,
            rows,
            &arrow_client_zstd,
            &batch,
            &mut insert_group,
            &rt,
        );

        insert_group.finish();

        // Benchmark arrow query
        let mut query_group = c.benchmark_group("QueryCompression");

        query_arrow(
            CompressionMethod::None,
            &arrow_table_ref,
            rows,
            &arrow_client_none,
            &mut query_group,
            &rt,
        );

        query_arrow(
            CompressionMethod::LZ4,
            &arrow_table_ref,
            rows,
            &arrow_client_lz4,
            &mut query_group,
            &rt,
        );

        query_arrow(
            CompressionMethod::ZSTD,
            &arrow_table_ref,
            rows,
            &arrow_client_zstd,
            &mut query_group,
            &rt,
        );

        query_group.finish();
    }

    if std::env::var(common::DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }

    // Shutdown container
    rt.block_on(ch.shutdown()).expect("Shutting down container");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
