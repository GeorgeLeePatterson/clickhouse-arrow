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
        .build::<ArrowFormat>()
        .await?;

    // Setup database and table
    arrow_tests::setup_database(db, &client).await?;

    // Create RecordBatches
    let batches = (0..num_runs)
        .map(|_| arrow_tests::create_test_batch(common::ROWS, false))
        .collect::<Vec<_>>();

    // Setup table
    let table = arrow_tests::setup_table(&client, db, &batches[0].schema()).await?;

    // Insert data
    let query = format!("INSERT INTO {table} FORMAT Native");
    for (i, batch) in batches.into_iter().enumerate() {
        common::header(format!("Insert {}", i + 1));
        let start = Instant::now();

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
    }

    Ok(())
}
