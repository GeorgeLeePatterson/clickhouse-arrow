#![expect(unused_crate_dependencies)]
mod common;

use std::sync::Arc;

use arrow::datatypes::*;
use clickhouse::{Client as ClickhouseRsClient, Row as ClickhouseRow};
use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::{ClickHouseContainer, arrow_tests};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(ClickhouseRow, Clone, Serialize, Deserialize)]
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
        .with_compression(CompressionMethod::LZ4)
        .with_setting("wait_end_of_query", 0)
        .with_http_options(|http| http.with_port(ch.get_http_port()))
}

fn setup_clickhouse_rs(ch: &'static ClickHouseContainer) -> ClickhouseRsClient {
    ClickhouseRsClient::default()
        .with_url(ch.get_http_url())
        .with_user(&ch.user)
        .with_password(&ch.password)
        .with_database("default")
        .with_compression(clickhouse::Compression::Lz4)
    // .with_compression(clickhouse::Compression::None)
    // .with_option("output_format_parallel_formatting", "0")
}

async fn run(ch: &'static ClickHouseContainer, num_runs: usize) -> Result<()> {
    eprintln!("ClickHouse Native Port: {}", ch.get_native_port());

    // Setup clients
    let arrow_client = setup_arrow_client(ch).build::<ArrowFormat>().await?;
    let rs_client = setup_clickhouse_rs(ch);

    let start = Instant::now();
    let rows = 500_000_000;
    let query = format!("SELECT number FROM system.numbers_mt LIMIT {rows}");

    let schema = Arc::new(Schema::new(vec![Field::new("number", DataType::UInt64, false)]));

    for i in 0..num_runs {
        common::header(format!("Select run #{}", i + 1));
        select_data(&query, i, Arc::clone(&schema), &arrow_client, &rs_client).await?;
    }

    common::timing("ALL SELECTS", start);

    Ok(())
}

async fn select_data(
    query: &str,
    i: usize,
    schema: SchemaRef,
    client: &ArrowClient,
    rs_client: &ClickhouseRsClient,
) -> Result<()> {
    // ----
    // ARROW

    let run_start = Instant::now();

    // Select batch
    let _result = client
        .query_http(query, None, schema, None, Some(Qid::new()))
        .await
        .inspect_err(|error| {
            eprintln!("\nQuery error:\n{error:?}\n");
        })?;
    let arrow_final = run_start.elapsed().as_secs_f64();
    common::timing(format!("ARROW RUN {}", i + 1), run_start);

    // Pause for sampling
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // ----
    // CLICKHOUSE
    let run_start = Instant::now();
    let _result = rs_client.query(query).fetch_all::<ChTestRow>().await.unwrap();
    let rs_final = run_start.elapsed().as_secs_f64();
    common::timing(format!("RS RUN {}", i + 1), run_start);

    common::header(format!(
        "---> Differences: subtract = {}, div = {}",
        arrow_final - rs_final,
        arrow_final / rs_final,
    ));

    Ok(())
}
