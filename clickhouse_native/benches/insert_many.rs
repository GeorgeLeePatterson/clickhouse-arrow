#![expect(unused_crate_dependencies)]
mod common;

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_native::CompressionMethod;
use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::{arrow_tests, get_or_create_container};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use self::common::{init, print_msg};

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

#[allow(clippy::too_many_lines)]
fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Init tracing
    init();

    // Setup container once
    let ch = rt.block_on(get_or_create_container(None));
    print_msg("Created container");

    let mut insert_group = c.benchmark_group("InsertMany");

    // Test with different row counts
    let row_counts = vec![100_000, 200_000, 300_000, 400_000, 500_000];
    let batch_size = 4;

    for rows in row_counts {
        print_msg(format!("Running test for {rows} rows"));

        // Pre-create the batch and rows to avoid including this in benchmark time
        let batches: Vec<RecordBatch> = (0..batch_size)
            .map(|_| arrow_tests::create_test_batch(rows / batch_size, false))
            .collect::<Vec<_>>();
        let test_rows = common::create_test_rows(rows);
        let schema = batches[0].schema();

        // Setup clients
        let arrow_client_builder =
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_compression(CompressionMethod::None);
        let arrow_client = rt
            .block_on(arrow_client_builder.build::<ArrowFormat>())
            .expect("clickhouse native arrow setup");
        let rs_client = common::setup_clickhouse_rs(ch);

        // Setup database
        rt.block_on(arrow_tests::setup_database(common::TEST_DB_NAME, &arrow_client))
            .expect("setup database");

        // Setup tables
        let arrow_table_ref = rt
            .block_on(arrow_tests::setup_table(&arrow_client, common::TEST_DB_NAME, &schema))
            .expect("clickhouse rs table");
        let rs_table_ref = rt
            .block_on(arrow_tests::setup_table(&arrow_client, common::TEST_DB_NAME, &schema))
            .expect("clickhouse rs table");

        // Wrap clients in Arc for sharing across iterations
        let arrow_client = Arc::new(arrow_client);
        let rs_client = Arc::new(rs_client);

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
        common::insert_rs(
            &rs_table_ref,
            rows,
            rs_client.as_ref(),
            &test_rows,
            &mut insert_group,
            &rt,
        );
    }

    insert_group.finish();

    if std::env::var(common::DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }

    // Shutdown container
    rt.block_on(ch.shutdown()).expect("Shutting down container");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
