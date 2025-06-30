#![expect(unused_crate_dependencies)]
mod common;

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use futures_util::StreamExt;

const ROWS: usize = 500_000_000;

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
    eprintln!("ClickHouse Native Port: {}", ch.get_native_port());

    // Create arrow client
    let client = arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        .with_compression(CompressionMethod::LZ4);

    let client = client.build::<ArrowFormat>().await?;

    let query = format!("SELECT number FROM system.numbers_mt LIMIT {ROWS}");
    for _ in 0..num_runs {
        // Select batch
        let batches = client
            .query(&query, None)
            .await
            .inspect_err(|error| eprintln!("\nQuery error:\n{error:?}\n"))?
            .collect::<Vec<_>>()
            .await;
        let rows = batches.into_iter().map(|b| b.unwrap()).map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(rows, ROWS, "clickhouse arrow rows mismatch");
    }

    Ok(())
}
