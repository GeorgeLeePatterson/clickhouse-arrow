#![expect(unused_crate_dependencies)]
mod common;

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use futures_util::StreamExt;
use tokio::time::Instant;

const ROWS: usize = 500_000_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::any::Any + Send>> {
    common::run_example_with_cleanup(|ch| async move { run(ch).await.unwrap() }, None).await?;
    Ok(())
}

async fn run(ch: &'static ClickHouseContainer) -> Result<()> {
    // Create arrow client
    let client = arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        .build::<ArrowFormat>()
        .await?;

    let query = format!("SELECT number FROM system.numbers_mt LIMIT {ROWS}");

    for _ in 0..5 {
        let start = Instant::now();

        let batches = client.query(&query, None).await?.collect::<Vec<_>>().await;
        let rows = batches.into_iter().map(|b| b.unwrap()).map(|b| b.num_rows()).sum::<usize>();

        assert_eq!(rows, ROWS, "clickhouse arrow rows mismatch");
        eprintln!("Queried {ROWS} rows in {:#?}", start.elapsed());
    }

    Ok(())
}
