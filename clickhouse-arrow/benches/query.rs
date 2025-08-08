#![expect(unused_crate_dependencies)]
mod common;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::CompressionMethod;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{arrow_tests, get_or_create_container};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::StreamExt;
use tokio::runtime::Runtime;

use self::common::{init, print_msg};

async fn insert_arrow(table: &str, client: &ArrowClient, batch: RecordBatch) {
    let query = format!("INSERT INTO {table} FORMAT NATIVE");
    let mut stream = client
        .insert(query, batch, None)
        .await
        .inspect_err(|e| print_msg(format!("Insert error\n{e:?}")))
        .unwrap();
    while let Some(r) = stream.next().await {
        r.unwrap();
    }
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

    let mut query_group = c.benchmark_group("Query");

    // Test with different row counts
    let row_counts = vec![10_000, 100_000, 200_000, 300_000, 400_000];

    for rows in row_counts {
        print_msg(format!("Running test for {rows} rows"));

        // Pre-create the batch and rows to avoid including this in benchmark time
        let batch = arrow_tests::create_test_batch(rows, false);
        let schema = batch.schema();

        // Setup clients
        let arrow_client_builder =
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true)
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

        // Insert into each table
        rt.block_on(insert_arrow(&arrow_table_ref, arrow_client.as_ref(), batch.clone()));
        rt.block_on(insert_arrow(&rs_table_ref, arrow_client.as_ref(), batch.clone()));

        // Benchmark native arrow query
        query_arrow(&arrow_table_ref, rows, arrow_client.as_ref(), &mut query_group, &rt);

        // Benchmark clickhouse-rs query
        common::query_rs(&rs_table_ref, rows, rs_client.as_ref(), &mut query_group, &rt);
    }

    query_group.finish();

    if std::env::var(common::DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }

    // Shutdown container
    rt.block_on(ch.shutdown()).expect("Shutting down container");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
