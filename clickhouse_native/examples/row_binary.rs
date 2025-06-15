#![expect(unused_crate_dependencies)]
mod common;

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use clickhouse::{Client as ClickhouseRsClient, Row as ClickhouseRow};
use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::{ClickHouseContainer, arrow_tests};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

const ROWS: usize = 500_000_000;

#[derive(ClickhouseRow, Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChTestRow {
    number: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::any::Any + Send>> {
    common::run_example_with_cleanup(
        |ch, num| async move {
            run(ch, num).await.unwrap();
        },
        None,
    )
    .await?;
    Ok(())
}

fn setup_arrow_client(ch: &'static ClickHouseContainer) -> ClientBuilder {
    arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        .with_compression(CompressionMethod::None)
        // .with_compression(CompressionMethod::LZ4)
        .with_http_options(|opts| opts.with_port(ch.get_http_port()))
}

fn setup_clickhouse_rs(ch: &'static ClickHouseContainer) -> ClickhouseRsClient {
    ClickhouseRsClient::default()
        .with_url(ch.get_http_url())
        .with_user(&ch.user)
        .with_password(&ch.password)
        .with_database("default")
        // .with_compression(clickhouse::Compression::Lz4)
        .with_compression(clickhouse::Compression::None)
}

#[allow(clippy::cast_precision_loss)]
async fn run(ch: &'static ClickHouseContainer, num_runs: usize) -> Result<()> {
    eprintln!("ClickHouse Native Port: {}", ch.get_native_port());

    // Setup clients
    let arrow_client = setup_arrow_client(ch).build::<ArrowFormat>().await?;
    let rs_client = setup_clickhouse_rs(ch);

    let start = Instant::now();
    let query = format!("SELECT number FROM system.numbers_mt LIMIT {ROWS}");

    let schema = Arc::new(Schema::new(vec![Field::new("number", DataType::UInt64, false)]));

    let mut arrow_runs = Vec::with_capacity(num_runs);
    let mut rs_runs = Vec::with_capacity(num_runs);

    // Warmup
    for _ in 0..3 {
        let _ = select_rs(&query, 999, &rs_client).await?;
        let _ = select_arrow(&query, 999, &arrow_client, Arc::clone(&schema)).await?;
    }

    for i in 0..num_runs {
        common::header(format!("Select run #{}", i + 1));
        let rs_final = select_rs(&query, i, &rs_client).await?;
        let arrow_final = select_arrow(&query, i, &arrow_client, Arc::clone(&schema)).await?;
        common::header(format!(
            "---> Differences: subtract = {}, div = {}",
            arrow_final - rs_final,
            arrow_final / rs_final,
        ));
        arrow_runs.push(arrow_final);
        rs_runs.push(rs_final);
    }

    common::timing("ALL SELECTS", start);

    eprintln!("\n---- STATS ----");

    // Max/Min/Avg
    let arrow_max = arrow_runs.iter().copied().fold(0_f64, f64::max);
    let rs_max = rs_runs.iter().copied().fold(0_f64, f64::max);
    let arrow_min = arrow_runs.iter().copied().fold(1_000_f64, f64::min);
    let rs_min = rs_runs.iter().copied().fold(1_000_f64, f64::min);
    let arrow_avg = arrow_runs.iter().sum::<f64>() / arrow_runs.len() as f64;
    let rs_avg = rs_runs.iter().sum::<f64>() / rs_runs.len() as f64;

    eprintln!("Arrow:\n  Min: {arrow_min} - Avg: {arrow_avg} - Max: {arrow_max}");
    eprintln!("RS:\n  Min: {rs_min} - Avg: {rs_avg} - Max: {rs_max}");

    Ok(())
}

async fn select_arrow(
    query: &str,
    i: usize,
    client: &ArrowClient,
    schema: SchemaRef,
) -> Result<f64> {
    let run_start = Instant::now();
    let batches = client
        .query_http(query, None, schema, None, Some(Qid::new()))
        .await
        .inspect_err(|error| {
            eprintln!("\nQuery error:\n{error:?}\n");
        })?;
    let arrow_final = run_start.elapsed().as_secs_f64();
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    common::timing(format!("ARROW RUN {}", i + 1), run_start);
    assert_eq!(rows, ROWS, "clickhouse arrow rows mismatch");

    Ok(arrow_final)
}

async fn select_rs(query: &str, i: usize, rs_client: &ClickhouseRsClient) -> Result<f64> {
    let run_start = Instant::now();
    let result = rs_client.query(query).fetch_all::<ChTestRow>().await.unwrap();
    let rs_final = run_start.elapsed().as_secs_f64();
    let rows = result.len();
    common::timing(format!("RS RUN {}", i + 1), run_start);
    assert_eq!(rows, ROWS, "clickhouse.rs rows mismatch");
    Ok(rs_final)
}
