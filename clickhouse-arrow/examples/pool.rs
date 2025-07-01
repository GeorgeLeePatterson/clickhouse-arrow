#![expect(unused_crate_dependencies)]
mod common;

use arrow::array::RecordBatch;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::any::Any + Send>> {
    common::run_example_with_cleanup(|ch| async move { run(ch).await.unwrap() }, None).await?;
    Ok(())
}

async fn run(ch: &'static ClickHouseContainer) -> Result<()> {
    let db = common::DB_NAME;

    // Setup clients
    let builder = arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        .with_compression(CompressionMethod::LZ4);

    let pool_size = 10;

    let pool = arrow_tests::setup_test_arrow_pool(builder, pool_size, None).await?;

    let manager = pool.get().await.unwrap();

    // Setup database and table
    arrow_tests::setup_database(db, &manager).await?;

    // Create RecordBatches
    let batches: Vec<RecordBatch> = (0..pool_size)
        .map(|_| arrow_tests::create_test_batch(common::ROWS, false))
        .collect::<Vec<_>>();

    // Setup table
    let table = arrow_tests::setup_table(&manager, db, &batches[0].schema()).await?;

    // Insert data
    let query = format!("INSERT INTO {table} FORMAT Native");
    for batch in batches {
        // Get client from pool
        let client = pool.get().await.unwrap();

        let mut stream = client
            .insert(query.as_str(), batch, None)
            .await
            .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
            .unwrap();

        while let Some(result) = stream.next().await {
            result?;
        }

        eprintln!();
    }

    // Query data
    for i in 0..pool_size {
        let offset = i as usize * common::ROWS;

        // Get client from pool
        let client = pool.get().await.unwrap();
        let query = format!("SELECT * FROM {table} LIMIT {offset},{}", common::ROWS);
        let mut stream = client.query(query, Some(Qid::new())).await?;
        while let Some(maybe_batch) = stream.next().await {
            arrow::util::pretty::print_batches(&[maybe_batch?.slice(0, 3)])?;
        }
    }

    Ok(())
}
