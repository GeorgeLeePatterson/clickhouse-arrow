#![expect(unused_crate_dependencies)]
mod common;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use clickhouse::{Client as ClickHouseRsClient, Row as ClickHouseRow};
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

const ROWS: usize = 500_000_000;

#[derive(ClickHouseRow, Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChTestRow {
    number: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::any::Any + Send>> {
    common::run_example_with_tracy(
        |ch, num, cl| async move {
            run(ch, num, cl).await.unwrap();
        },
        None,
    )
    .await?;
    Ok(())
}

fn setup_arrow_client(ch: &'static ClickHouseContainer) -> ClientBuilder {
    let b = arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        // .with_compression(CompressionMethod::None)
        .with_compression(CompressionMethod::LZ4);

    #[cfg(feature = "row_binary")]
    let b = b.with_http_options(|opts| opts.with_port(ch.get_http_port()));

    b
}

#[allow(unused)]
fn setup_clickhouse_rs(ch: &'static ClickHouseContainer) -> ClickHouseRsClient {
    ClickHouseRsClient::default()
        .with_url(ch.get_http_url())
        .with_user(&ch.user)
        .with_password(&ch.password)
        .with_database("default")
        .with_compression(clickhouse::Compression::Lz4)
    // .with_compression(clickhouse::Compression::None)
}

#[allow(clippy::cast_precision_loss)]
async fn run(
    ch: &'static ClickHouseContainer,
    num_runs: usize,
    client: tracy_client::Client,
) -> Result<()> {
    eprintln!("ClickHouse Native Port: {}", ch.get_native_port());

    let builder = setup_arrow_client(ch);
    let comp = builder.options().compression;
    eprintln!("Using compression: {comp:?}");

    let arrow_client = builder.build::<ArrowFormat>().await?;

    let query = format!("SELECT number FROM system.numbers_mt LIMIT {ROWS}");
    let schema = Arc::new(Schema::new(vec![Field::new("number", DataType::UInt64, false)]));

    let rs_client = setup_clickhouse_rs(ch);
    let mut arrow_runs = Vec::with_capacity(num_runs);
    let mut rs_runs = Vec::with_capacity(num_runs);

    // Warmup
    for _ in 0..3 {
        let _ = select_arrow(&query, 999, &arrow_client, Arc::clone(&schema), None).await?;
        let _ = select_rs(&query, 999, &rs_client, None).await?;
    }

    let start = Instant::now();

    let loop_guard = client.non_continuous_frame(tracy_client::frame_name!("main_loop"));

    for i in 0..num_runs {
        common::header(format!("Select run #{}", i + 1));

        let arrow_final =
            select_arrow(&query, i, &arrow_client, Arc::clone(&schema), Some(&client)).await?;

        client.frame_mark(); // Mark frame boundary

        let rs_final = select_rs(&query, i, &rs_client, Some(&client)).await?;

        client.frame_mark(); // Mark frame boundary

        eprintln!(">> diff = {}, div = {}", arrow_final - rs_final, arrow_final / rs_final);
        arrow_runs.push(arrow_final);
        rs_runs.push(rs_final);
    }

    drop(loop_guard);

    common::timing("ALL SELECTS", start);

    eprintln!("---------------");
    eprintln!("---- STATS ----");

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
    arrow_client: &ArrowClient,
    _schema: SchemaRef,
    client: Option<&tracy_client::Client>,
) -> Result<f64> {
    let guard = client.map(|c| c.non_continuous_frame(tracy_client::frame_name!("run_loop_arrow")));
    let run_start = Instant::now();

    // TODO: Remove
    // let batches = arrow_client
    //     .query_http(query, None, schema, None, Some(Qid::new()))
    //     .await
    //     .inspect_err(|error| {
    //         eprintln!("\nQuery error:\n{error:?}\n");
    //     })?;

    let stream = arrow_client.query(query, None).await.inspect_err(|error| {
        eprintln!("\nQuery error:\n{error:?}\n");
    })?;
    let batches = stream.collect::<Vec<_>>().await;
    drop(guard);

    let arrow_final = run_start.elapsed().as_secs_f64();
    let rows = batches.into_iter().map(|b| b.unwrap()).map(|b| b.num_rows()).sum::<usize>();
    // let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    common::timing(format!("ARROW RUN {}", i + 1), run_start);
    assert_eq!(rows, ROWS, "clickhouse arrow rows mismatch");

    Ok(arrow_final)
}

// TODO: Remove - cleanup examples
#[allow(unused)]
async fn select_rs(
    query: &str,
    i: usize,
    rs_client: &ClickHouseRsClient,
    client: Option<&tracy_client::Client>,
) -> Result<f64> {
    let guard = client.map(|c| c.non_continuous_frame(tracy_client::frame_name!("run_loop_rs")));
    let run_start = Instant::now();

    let result = rs_client.query(query).fetch_all::<ChTestRow>().await.unwrap();
    drop(guard);

    let rs_final = run_start.elapsed().as_secs_f64();
    let rows = result.len();
    common::timing(format!("RS RUN {}", i + 1), run_start);
    assert_eq!(rows, ROWS, "clickhouse.rs rows mismatch");
    Ok(rs_final)
}
