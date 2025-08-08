#![expect(unused_crate_dependencies)]
mod common;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{arrow_tests, get_or_create_container};
use clickhouse_arrow::{CompressionMethod, ConnectionPool};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use self::common::{init, print_msg};

fn insert_arrow(
    compression: &str,
    table: &str,
    rows: usize,
    pool: &ConnectionPool<ArrowFormat>,
    batch: &RecordBatch,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    // Benchmark native arrow insert
    let query = format!("INSERT INTO {table} FORMAT NATIVE");
    let _ = group
        // Reduce sample size for slower operations
        .sample_size(50)
        .bench_with_input(
            BenchmarkId::new(format!("clickhouse_arrow_{compression}"), rows),
            &(&query, pool),
            |b, &(query, pool)| {
                b.to_async(rt).iter_batched(
                    || batch.clone(),
                    |batch| async move {
                        let client = pool.get().await.unwrap();
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

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Init tracing
    init();

    // Setup container once
    let ch = rt.block_on(get_or_create_container(None));
    print_msg("Created container");

    let mut insert_group = c.benchmark_group("PoolInsert");

    // Test with different row counts
    let row_counts = vec![10_000, 100_000, 200_000, 300_000, 400_000];

    // Create arrow pool outside of loop
    let arrow_client_builder =
        arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
            .with_ipv4_only(true)
            .with_compression(CompressionMethod::LZ4);

    // Pool
    #[expect(clippy::cast_possible_truncation)]
    let pool_size = common::DEFAULT_INSERT_SAMPLE_SIZE as u32 + 3;
    let arrow_pool = rt
        .block_on(arrow_tests::setup_test_arrow_pool(arrow_client_builder, pool_size, None))
        .expect("clickhouse native arrow setup");

    // Manage client
    let arrow_manage = rt.block_on(arrow_pool.dedicated_connection()).unwrap();

    for rows in row_counts {
        print_msg(format!("Running test for {rows} rows"));

        // Pre-create the batch and rows to avoid including this in benchmark time
        let batch = arrow_tests::create_test_batch(rows, false);
        let test_rows = common::create_test_rows(rows);
        let schema = batch.schema();

        // Setup clients
        let rs_client =
            common::setup_clickhouse_rs(ch).with_compression(clickhouse::Compression::Lz4);

        // Setup database
        rt.block_on(arrow_tests::setup_database(common::TEST_DB_NAME, &arrow_manage))
            .expect("setup database");

        // Setup tables
        let arrow_table_ref = rt
            .block_on(arrow_tests::setup_table(&arrow_manage, common::TEST_DB_NAME, &schema))
            .expect("clickhouse rs table");
        let rs_table_ref = rt
            .block_on(arrow_tests::setup_table(&arrow_manage, common::TEST_DB_NAME, &schema))
            .expect("clickhouse rs table");

        // Benchmark native arrow insert
        insert_arrow("lz4", &arrow_table_ref, rows, &arrow_pool, &batch, &mut insert_group, &rt);

        // Benchmark clickhouse-rs insert
        common::insert_rs(
            "lz4",
            &rs_table_ref,
            rows,
            &rs_client,
            &test_rows,
            &mut insert_group,
            &rt,
        );
    }

    insert_group.finish();

    // Drop the pool in the tokio executor, since bb8 uses tokio spawning
    rt.block_on(async move {
        drop(arrow_manage);
        drop(arrow_pool);
    });

    if std::env::var(common::DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }

    // Shutdown container
    rt.block_on(ch.shutdown()).expect("Shutting down container");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
