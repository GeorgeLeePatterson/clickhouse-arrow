#![expect(unused_crate_dependencies)]
mod common;

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse::{Client as ClickhouseRsClient, Row as ClickhouseRow};
use clickhouse_native::CompressionMethod;
use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::{arrow_tests, get_or_create_container};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use self::common::{init, print_msg};

#[derive(ClickhouseRow, Clone, Serialize, Deserialize)]
pub(crate) struct ChTestRow {
    number: u64,
}

pub(crate) fn query_rs_scalar(
    query: &str,
    rows: usize,
    client: &ClickhouseRsClient,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let _ = group.sample_size(10).measurement_time(Duration::from_secs(40)).bench_with_input(
        BenchmarkId::new("clickhouse_rowbinary", rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let result = client.query(query).fetch_all::<ChTestRow>().await.unwrap();
                black_box(result)
            });
        },
    );
}

fn query_arrow_all(
    query: &str,
    rows: usize,
    client: &ArrowClient,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let _ = group.sample_size(10).measurement_time(Duration::from_secs(40)).bench_with_input(
        BenchmarkId::new("clickhouse_arrow", rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let mut stream = client
                    .query(*query, None)
                    .await
                    .inspect_err(|e| print_msg(format!("Query error: {e:?}")))
                    .unwrap();
                let mut batches = Vec::with_capacity(100);
                while let Some(b) = stream.next().await {
                    batches
                        .push(b.inspect_err(|e| print_msg(format!("Batch error: {e:?}"))).unwrap());
                }
                black_box(batches)
            });
        },
    );
}

fn query_arrow(
    rows: usize,
    split: usize,
    client: &Arc<ArrowClient>,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let size = rows.div_ceil(split);
    let query = format!("SELECT number from system.numbers_mt LIMIT {size}");
    let _ = group.sample_size(10).measurement_time(Duration::from_secs(40)).bench_with_input(
        BenchmarkId::new(format!("clickhouse_arrow_{split}"), rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let mut tasks = tokio::task::JoinSet::<Vec<RecordBatch>>::new();
                for _ in 0..split {
                    let client = Arc::clone(client);
                    let query = query.to_string();
                    drop(tasks.spawn(async move {
                        let mut stream = client
                            .query(query, None)
                            .await
                            .inspect_err(|e| print_msg(format!("Query error: {e:?}")))
                            .unwrap();
                        let mut batches = Vec::with_capacity(1024);
                        while let Some(b) = stream.next().await {
                            batches.push(
                                b.inspect_err(|e| print_msg(format!("Batch error: {e:?}")))
                                    .unwrap(),
                            );
                        }
                        batches
                    }));
                }
                let mut all_batches = Vec::with_capacity(split * 1024);
                #[expect(clippy::disallowed_methods)]
                while let Some(batches) = tasks.join_next().await {
                    all_batches.extend(batches.unwrap());
                }
                black_box(all_batches)
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
    let ch = rt.block_on(get_or_create_container(None));
    print_msg("Created container");

    let mut query_group = c.benchmark_group("Scalar");

    // Test with different row counts
    let rows = 500_000_000;
    let query = format!("SELECT number FROM system.numbers_mt LIMIT {rows}");

    print_msg(format!("Running test for {rows} rows"));

    // Setup clients
    let arrow_client_builder =
        arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
            .with_compression(CompressionMethod::ZSTD);
    let arrow_client = rt
        .block_on(arrow_client_builder.build::<ArrowFormat>())
        .expect("clickhouse native arrow setup");

    let rs_client = common::setup_clickhouse_rs(ch)
        .with_compression(clickhouse::Compression::Lz4)
        .with_option("output_format_parallel_formatting", "0");

    // Wrap clients in Arc for sharing across iterations
    let arrow_client = Arc::new(arrow_client);
    let rs_client = Arc::new(rs_client);

    // Benchmark native arrow query
    query_arrow_all(&query, rows, &arrow_client, &mut query_group, &rt);

    // Benchmark clickhouse-rs query
    query_rs_scalar(&query, rows, rs_client.as_ref(), &mut query_group, &rt);

    // Benchmark native arrow query
    query_arrow(rows, 20, &arrow_client, &mut query_group, &rt);

    query_group.finish();

    if std::env::var(common::DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }

    rt.block_on(ch.shutdown()).expect("Shutting down container");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
