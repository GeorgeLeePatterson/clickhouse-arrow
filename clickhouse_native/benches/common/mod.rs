use std::hint::black_box;

use clickhouse::{Client as ClickhouseRsClient, Row as ClickhouseRow};
use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::{ClickHouseContainer, init_tracing};
use clickhouse_native::{CompressionMethod, Row, Uuid};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId};
use futures_util::{StreamExt, stream};
use klickhouse::{
    Client as KlickhouseClient, ClientOptions as KlientOptions, DateTime64 as KDateTime64,
    Row as KRow,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[allow(unused)]
pub(crate) const DISABLE_CLEANUP_ENV: &str = "DISABLE_CLEANUP";
#[allow(unused)]
pub(crate) const TEST_DB_NAME: &str = "benchmark_test";
#[allow(unused)]
pub(crate) const DEFAULT_INSERT_SAMPLE_SIZE: usize = 50;

#[derive(ClickhouseRow, Clone, Serialize, Deserialize)]
pub(crate) struct ClickhouseRsRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    i64, // DateTime64(3) maps to i64 milliseconds
}

#[derive(Row, Clone, Serialize, Deserialize)]
pub(crate) struct ClickhouseNativeRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    DateTime64<3>,
}

#[derive(KRow, Clone)]
pub(crate) struct KlickhouseRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    KDateTime64<3>, // DateTime64(3) maps to i64 milliseconds
}

pub(crate) fn init() {
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
pub(crate) fn setup_clickhouse_rs(ch: &'static ClickHouseContainer) -> ClickhouseRsClient {
    ClickhouseRsClient::default()
        .with_url(ch.get_http_url())
        .with_user(&ch.user)
        .with_password(&ch.password)
        .with_database("default")
}

#[allow(unused)]
pub(crate) async fn setup_klickhouse(ch: &'static ClickHouseContainer) -> Result<KlickhouseClient> {
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
#[allow(unused)]
#[expect(clippy::cast_precision_loss)]
#[expect(clippy::cast_possible_wrap)]
pub(crate) fn create_test_rows(rows: usize) -> Vec<ClickhouseRsRow> {
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
pub(crate) fn create_test_native_rows(rows: usize) -> Vec<ClickhouseNativeRow> {
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
#[allow(unused)]
#[expect(clippy::cast_precision_loss)]
#[expect(clippy::cast_possible_wrap)]
pub(crate) fn create_test_klickhouse_rows(rows: usize) -> Vec<KlickhouseRow> {
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

#[allow(unused)]
pub(crate) fn insert_rs(
    table: &str,
    rows: usize,
    client: &ClickhouseRsClient,
    batch: &[ClickhouseRsRow],
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let _ = group.sample_size(DEFAULT_INSERT_SAMPLE_SIZE).bench_with_input(
        BenchmarkId::new("clickhouse_rowbinary", rows),
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

#[allow(unused)]
pub(crate) fn insert_kh(
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
        .sample_size(DEFAULT_INSERT_SAMPLE_SIZE)
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

#[allow(unused)]
pub(crate) fn query_rs(
    table: &str,
    rows: usize,
    client: &ClickhouseRsClient,
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
) {
    let query = format!("SELECT * FROM {table} LIMIT {rows}");
    let _ = group.bench_with_input(
        BenchmarkId::new("clickhouse_rowbinary", rows),
        &(query, client),
        |b, (query, client)| {
            b.to_async(rt).iter(|| async move {
                let result = client.query(query).fetch_all::<ClickhouseRsRow>().await.unwrap();
                black_box(result)
            });
        },
    );
}

#[allow(unused)]
pub(crate) fn query_kh(
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
