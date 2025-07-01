#![expect(unused_crate_dependencies)]
mod common;

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
    let client = arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        .with_compression(CompressionMethod::LZ4)
        .build::<ArrowFormat>()
        .await?;

    // Setup database and table
    arrow_tests::setup_database(db, &client).await?;

    let batch = arrow_tests::create_test_batch(100, false);

    // Setup table
    let table = arrow_tests::setup_table(&client, db, &batch.schema()).await?;

    // Insert data
    let mut stream = client
        .insert(format!("INSERT INTO {table} FORMAT Native"), batch, Some(Qid::new()))
        .await
        .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
        .unwrap();

    while let Some(result) = stream.next().await {
        result?;
    }

    let batch = client
        .query(format!("SELECT * FROM {table} LIMIT 10"), None)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .remove(0);

    assert_eq!(batch.num_rows(), 10);

    Ok(())
}
