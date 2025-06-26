#![expect(unused_crate_dependencies)]
mod common;

use clickhouse::{Client as ClickHouseRsClient, Row as ClickHouseRow};
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

const INSERTS: usize = 5;

#[derive(ClickHouseRow, Clone, Serialize, Deserialize)]
pub(crate) struct ChTestRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    i64, // DateTime64(3) maps to i64 milliseconds
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

fn setup_clickhouse_rs(url: &str, user: &str, password: &str) -> ClickHouseRsClient {
    ClickHouseRsClient::default()
        .with_url(url)
        .with_user(user)
        .with_password(password)
        .with_database("default")
        .with_compression(clickhouse::Compression::Lz4)
        .with_option("output_format_parallel_formatting", "0")
}

async fn run(ch: &'static ClickHouseContainer, num_runs: usize) -> Result<()> {
    let db = common::DB_NAME;

    // Setup clients
    let native_url = ch.get_native_url();
    let http_url = ch.get_http_url();
    let user = &ch.user;
    let pass = &ch.password;
    let arrow_client = arrow_tests::setup_test_arrow_client(native_url, user, pass)
        .with_compression(CompressionMethod::LZ4)
        .with_ext(|ext| {
            ext.with_chunked_send_mode(ChunkedProtocolMode::ChunkedOptional)
                .with_chunked_recv_mode(ChunkedProtocolMode::ChunkedOptional)
        })
        .with_setting("preferred_block_size_bytes", 512 * 1024)
        .build::<ArrowFormat>()
        .await?;
    let rs_client = setup_clickhouse_rs(&http_url, user, pass);

    // Setup database and table
    arrow_tests::setup_database(db, &arrow_client).await?;
    let schema_batch = arrow_tests::create_test_batch(1, false);

    // Setup table
    let table = arrow_tests::setup_table(&arrow_client, db, &schema_batch.schema()).await?;

    eprintln!("\n\n\n\n\n");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // -- INSERT
    let mut insert_tasks = tokio::task::JoinSet::<()>::new();
    for _ in 0..INSERTS {
        let table = table.clone();
        let arrow_client = arrow_client.clone();
        drop(insert_tasks.spawn(async move {
            let qid = Qid::new();

            // Create and insert batches
            let total_rows = common::ROWS * 50;
            common::header(format!("Creating test batch w/ {total_rows} rows"));
            let batch = arrow_tests::create_test_batch(total_rows, false);
            common::header(format!("Created test batch w/ {total_rows} rows"));

            // Insert test data
            common::header(format!("Inserting batch: {total_rows} rows - {qid}"));
            let mut stream = arrow_client
                .insert_max_rows(
                    format!("INSERT INTO {table} FORMAT Native"),
                    batch,
                    100_000,
                    Some(qid),
                )
                .await
                .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
                .unwrap();

            while let Some(result) = stream.next().await {
                result.unwrap();
            }
            common::header(format!("Inserted {total_rows} Rows: {qid}"));
        }));
    }

    #[allow(clippy::disallowed_methods)]
    while let Some(result) = insert_tasks.join_next().await {
        result.unwrap();
    }

    // -- SELECT
    let mut times: Vec<(f64, f64)> = Vec::with_capacity(num_runs);
    let start = Instant::now();
    let query = format!("SELECT * FROM {table}");
    for i in 0..num_runs {
        common::header(format!("Select run #{}", i + 1));
        let results = select_data(&query, i, &arrow_client, &rs_client).await?;
        times.push(results);
    }
    common::timing("ALL SELECTS", start);

    // -- STATS
    eprintln!();
    for (arrow_t, ch_t) in &times {
        eprintln!(
            "Arrow = {:#?}, Ch = {:#?}",
            std::time::Duration::from_secs_f64(*arrow_t),
            std::time::Duration::from_secs_f64(*ch_t)
        );
    }

    #[expect(clippy::cast_precision_loss)]
    let (arrow_avg, ch_avg) = (
        times.iter().map(|(a, _)| a).sum::<f64>() / num_runs as f64,
        times.iter().map(|(_, c)| c).sum::<f64>() / num_runs as f64,
    );

    eprintln!();
    eprintln!(
        "Avg: Arrow = {:#?}, Ch = {:#?}",
        std::time::Duration::from_secs_f64(arrow_avg),
        std::time::Duration::from_secs_f64(ch_avg)
    );

    let run_clickhouse_client = format!(
        "clickhouse client --host localhost --port {} --user \"{user}\" --password \"{pass}\" -f \
         native --query=\"{query}\" > /dev/null",
        ch.get_native_port()
    );
    eprintln!("\n\nClickHouse client test:\n{run_clickhouse_client}");

    Ok(())
}

async fn select_data(
    query: &str,
    i: usize,
    client: &ArrowClient,
    rs_client: &ClickHouseRsClient,
) -> Result<(f64, f64)> {
    // ----
    // ARROW

    let run_start = Instant::now();

    // Select batch
    let mut stream = client.query(query, None).await.inspect_err(|error| {
        eprintln!("\nQuery error:\n{error:?}\n");
    })?;
    common::timing("  >> Query", run_start);
    let wait_start = Instant::now();

    // Stream results
    let mut iter = 0;
    let mut result_start = Instant::now();
    while let Some(maybe_batch) = stream.next().await {
        if iter == 0 {
            common::timing("  >> WAIT First Batch", wait_start);
        }

        iter += 1;
        let batch = maybe_batch.inspect_err(|error| eprintln!("\nBatch error:\n{error:?}\n"))?;
        let rows = batch.num_rows();
        common::timing(format!("  >> Batch {iter} (rows = {rows})"), result_start);
        result_start = Instant::now();
    }

    let arrow_final = run_start.elapsed().as_secs_f64();
    common::timing(format!("ARROW RUN {}", i + 1), run_start);

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

    Ok((arrow_final, rs_final))
}
