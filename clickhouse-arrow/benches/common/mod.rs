use std::hint::black_box;

use clickhouse::{Client as ClickHouseRsClient, Row as ClickHouseRow};
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, init_tracing};
use clickhouse_arrow::{CompressionMethod, Row, Uuid};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[allow(unused)]
pub(crate) const DISABLE_CLEANUP_ENV: &str = "DISABLE_CLEANUP";
#[allow(unused)]
pub(crate) const TEST_DB_NAME: &str = "benchmark_test";
#[allow(unused)]
pub(crate) const DEFAULT_INSERT_SAMPLE_SIZE: usize = 50;

#[derive(ClickHouseRow, Clone, Serialize, Deserialize)]
pub(crate) struct ClickHouseRsRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    i64, // DateTime64(3) maps to i64 milliseconds
}

#[derive(Row, Clone, Serialize, Deserialize)]
pub(crate) struct ClickHouseNativeRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    DateTime64<3>,
}

pub(crate) fn init() {
    // TODO: Remove this lint when let chains is stable
    #[allow(clippy::collapsible_if)]
    if let Ok(l) = std::env::var("RUST_LOG") {
        if !l.is_empty() {
            // Add directives here
            init_tracing(Some(&[/*("tokio", "error")*/]));
        }
    }
}

pub(crate) fn print_msg(msg: impl std::fmt::Display) {
    eprintln!("\n--------\n{msg}\n--------\n\n");
}

#[allow(unused)]
pub(crate) async fn setup_clickhouse_native(
    ch: &'static ClickHouseContainer,
) -> Result<NativeClient> {
    Client::<NativeFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_compression(CompressionMethod::None)
        .build()
        .await
}

#[allow(unused)]
pub(crate) fn setup_clickhouse_rs(ch: &'static ClickHouseContainer) -> ClickHouseRsClient {
    ClickHouseRsClient::default()
        .with_url(ch.get_http_url())
        .with_user(&ch.user)
        .with_password(&ch.password)
        .with_database("default")
}

// Helper function to create test rows for clickhouse-rs
#[allow(unused)]
#[expect(clippy::cast_precision_loss)]
#[expect(clippy::cast_possible_wrap)]
pub(crate) fn create_test_rows(rows: usize) -> Vec<ClickHouseRsRow> {
    (0..rows)
        .map(|i| ClickHouseRsRow {
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
pub(crate) fn create_test_native_rows(rows: usize) -> Vec<ClickHouseNativeRow> {
    (0..rows)
        .map(|i| ClickHouseNativeRow {
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

#[allow(unused)]
pub(crate) fn insert_rs(
    bench_id: &str,
    table: &str,
    rows: usize,
    client: &ClickHouseRsClient,
    batch: &[ClickHouseRsRow],
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let id = if bench_id.is_empty() {
        "clickhouse_rowbinary"
    } else {
        &format!("clickhouse_rowbinary_{bench_id}")
    };
    let _ = group
        .sample_size(DEFAULT_INSERT_SAMPLE_SIZE)
        .measurement_time(std::time::Duration::from_secs(30))
        .bench_with_input(BenchmarkId::new(id, rows), &(table, client), |b, (table, client)| {
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
        });
}

#[allow(unused)]
pub(crate) fn query_rs(
    table: &str,
    rows: usize,
    client: &ClickHouseRsClient,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let query = format!("SELECT * FROM {table} LIMIT {rows}");
    let _ = group.bench_with_input(
        BenchmarkId::new("clickhouse_rowbinary", rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let result = client.query(query).fetch_all::<ClickHouseRsRow>().await.unwrap();
                black_box(result)
            });
        },
    );
}
