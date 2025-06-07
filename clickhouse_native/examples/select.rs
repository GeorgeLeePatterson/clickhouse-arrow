#![expect(unused_crate_dependencies)]
mod common;

use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::{ClickHouseContainer, arrow_tests};
use futures_util::StreamExt;
use tokio::time::Instant;

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

async fn run(ch: &'static ClickHouseContainer, num_runs: usize) -> Result<()> {
    let db = common::DB_NAME;

    // Setup clients
    let client = arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        .with_compression(CompressionMethod::LZ4)
        .with_ext(|ext| {
            ext.with_chunked_send_mode(ChunkedProtocolMode::ChunkedOptional)
                .with_chunked_recv_mode(ChunkedProtocolMode::ChunkedOptional)
        })
        .build::<ArrowFormat>()
        .await?;

    // Setup database and table
    arrow_tests::setup_database(db, &client).await?;

    // Create batches
    let total_rows = common::ROWS * num_runs;
    let batch = arrow_tests::create_test_batch(total_rows, false);

    // Setup table
    let table = arrow_tests::setup_table(&client, db, &batch.schema()).await?;

    common::header(format!("Inserting {total_rows} Rows"));

    // Insert test data
    let mut stream = client
        .insert_max_rows(format!("INSERT INTO {table} FORMAT Native"), batch, 100_000, None)
        .await
        .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
        .unwrap();
    while let Some(result) = stream.next().await {
        result?;
    }

    // Pause for sampling
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let start = Instant::now();

    for i in 0..num_runs {
        let offset = i * common::ROWS;
        common::header(format!("Select run #{}: {offset} - {}", i + 1, common::ROWS));
        let query = format!("SELECT * FROM {table} LIMIT {offset},{} FORMAT Native", common::ROWS);
        select_data(&query, i, &client).await?;
    }

    common::timing("ALL SELECTS", start);

    Ok(())
}

async fn select_data(query: &str, i: usize, client: &ArrowClient) -> Result<()> {
    let run_start = Instant::now();

    // Select batch
    let mut stream = client.query(query, Some(Qid::new())).await?;

    // Stream results
    let mut iter = 0;
    let mut result_start = Instant::now();
    while let Some(maybe_batch) = stream.next().await {
        iter += 1;
        let batch = maybe_batch?;
        let rows = batch.num_rows();

        common::timing(format!("  >> Batch {iter} (rows = {rows})"), result_start);
        result_start = Instant::now();
    }

    common::timing(format!("RUN {}", i + 1), run_start);

    Ok(())
}
