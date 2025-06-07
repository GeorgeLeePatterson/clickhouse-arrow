#![expect(unused_crate_dependencies)]
mod common;

use std::time::Duration;

use arrow::array::RecordBatch;
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
    let all_start = Instant::now();

    // Setup clients
    let builder = arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        .with_compression(CompressionMethod::LZ4);

    #[expect(clippy::cast_possible_truncation)]
    let pool = arrow_tests::setup_test_arrow_pool(builder, num_runs as u32 * 2, None).await?;

    let manager = pool.get().await.unwrap();

    // Setup database and table
    arrow_tests::setup_database(db, &manager).await?;

    // Create RecordBatches
    let batches: Vec<RecordBatch> = (0..num_runs)
        .map(|_| arrow_tests::create_test_batch(common::ROWS, false))
        .collect::<Vec<_>>();

    // Setup table
    let table = arrow_tests::setup_table(&manager, db, &batches[0].schema()).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let total = batches.len();

    // Insert data
    let query = format!("INSERT INTO {table} FORMAT Native");
    for (i, batch) in batches.into_iter().enumerate() {
        let start = Instant::now();

        // Get client from pool
        let client = pool.get().await.unwrap();
        common::header(format!("Insert {} (client={})", i + 1, client.client_id));

        let mut stream = client
            .insert(query.as_str(), batch, None)
            .await
            .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
            .unwrap();
        common::timing(format!("Insert {}", i + 1), start);

        while let Some(result) = stream.next().await {
            result?;
        }

        common::timing(format!("Total Insert {}", i + 1), start);
        eprintln!();
    }

    // Query data
    for i in 0..total {
        let offset = i * common::ROWS;

        // Get client from pool
        let client = pool.get().await.unwrap();
        common::header(format!("Select {} (client={})", i + 1, client.client_id));

        let query = format!("SELECT * FROM {table} LIMIT {offset},{}", common::ROWS);
        eprintln!(">> Running query: {query}");

        let mut stream = client.query(query, Some(Qid::new())).await?;
        while let Some(maybe_batch) = stream.next().await {
            let batch = maybe_batch?;
            arrow::util::pretty::print_batches(&[batch.slice(0, 3)])?;
        }
    }

    common::timing("** TOTAL RUNNING", all_start);

    Ok(())
}
