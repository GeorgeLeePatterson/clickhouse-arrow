#![expect(unused_crate_dependencies)]
mod common;

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use futures_util::StreamExt;

const INSERTS: usize = 3;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::any::Any + Send>> {
    common::run_example_with_cleanup(|ch| async move { run(ch).await.unwrap() }, None).await?;
    Ok(())
}

#[expect(clippy::disallowed_methods)]
async fn run(ch: &'static ClickHouseContainer) -> Result<()> {
    let db = common::DB_NAME;

    // Setup clients
    let native_url = ch.get_native_url();
    let arrow_client = arrow_tests::setup_test_arrow_client(native_url, &ch.user, &ch.password)
        .with_compression(CompressionMethod::LZ4)
        // Example for how to configure chunked protocol mode
        .with_ext(|ext| {
            ext.with_chunked_send_mode(ChunkedProtocolMode::ChunkedOptional)
                .with_chunked_recv_mode(ChunkedProtocolMode::ChunkedOptional)
        })
        .build::<ArrowFormat>()
        .await?;

    // Setup database and table
    arrow_tests::setup_database(db, &arrow_client).await?;
    let schema_batch = arrow_tests::create_test_batch(1, false);

    // Setup table
    let table = arrow_tests::setup_table(&arrow_client, db, &schema_batch.schema()).await?;

    // INSERT
    let mut insert_tasks = tokio::task::JoinSet::<()>::new();
    for _ in 0..INSERTS {
        let table = table.clone();
        let arrow_client = arrow_client.clone();
        drop(insert_tasks.spawn(async move {
            let qid = Qid::new();

            // Create and insert batches
            let batches = (0..50)
                .map(|_| arrow_tests::create_test_batch(common::ROWS, false))
                .collect::<Vec<_>>();

            // Insert test data
            let mut stream = arrow_client
                .insert_many(format!("INSERT INTO {table} FORMAT Native"), batches, Some(qid))
                .await
                .unwrap();

            while let Some(result) = stream.next().await {
                result.unwrap();
            }

            eprintln!("Inserted {} Rows: {qid}", common::ROWS * 50);
        }));
    }

    #[allow(clippy::disallowed_methods)]
    while let Some(result) = insert_tasks.join_next().await {
        result.unwrap();
    }

    // SELECT
    let mut tasks: Vec<_> = Vec::with_capacity(INSERTS);
    for i in 0..3 {
        let table = table.clone();
        let client = arrow_client.clone();
        tasks.push(tokio::spawn(async move {
            // Select batch
            let query = format!("SELECT * FROM {table}");
            let mut stream = client.query(query, None).await.unwrap();

            // Stream results
            let mut total_rows = 0;
            while let Some(maybe_batch) = stream.next().await {
                total_rows += maybe_batch.unwrap().num_rows();
            }
            eprintln!("Task {i}, total rows = {total_rows}");
        }));
    }

    // Wait for all SELECT tasks to complete
    for result in tasks {
        result.await.unwrap();
    }

    Ok(())
}
