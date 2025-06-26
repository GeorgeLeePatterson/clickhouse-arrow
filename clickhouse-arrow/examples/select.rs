#![expect(unused_crate_dependencies)]
mod common;

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
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

    let dummy_batch = arrow_tests::create_test_batch(1, false);

    // Setup table
    let table = arrow_tests::setup_table(&client, db, &dummy_batch.schema()).await?;

    // TODO: Remove
    let mut insert_times = vec![];

    // Create batches
    let total_rows = common::ROWS;
    for i in 0..50 {
        let start = Instant::now();
        let batch = arrow_tests::create_test_batch(total_rows, false);

        common::header(format!("Inserting {total_rows} Rows"));
        // Insert test data
        let mut stream = client
            .insert(format!("INSERT INTO {table} FORMAT Native"), batch, None)
            .await
            .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
            .unwrap();
        while let Some(result) = stream.next().await {
            result?;
        }

        // TODO: Remove
        let elapsed = start.elapsed();
        eprintln!("Inserted batch {i} in {elapsed:#?}");
        insert_times.push(elapsed.as_secs_f64());
    }

    // TODO: Remove
    #[allow(clippy::cast_precision_loss)]
    let insert_avg = insert_times.iter().sum::<f64>() / insert_times.len() as f64;
    eprintln!("Average insert time: {insert_avg:#?}");

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

        arrow::util::pretty::print_batches(&[batch.slice(0, 3.min(rows))]).unwrap();

        // TODO: Remove
        common::timing(format!("  >> Batch {iter} (rows = {rows})"), result_start);
        result_start = Instant::now();
    }

    common::timing(format!("RUN {}", i + 1), run_start);

    Ok(())
}
