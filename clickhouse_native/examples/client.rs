#![expect(unused_crate_dependencies)]
use std::time::Duration;

use arrow::array::RecordBatch;
use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::{arrow_tests, get_or_create_container, init_tracing};
use futures_util::StreamExt;

fn init() {
    if let Ok(l) = std::env::var("RUST_LOG") {
        if !l.is_empty() {
            // Add directives here
            init_tracing(Some(&[/* ("tokio", "error") */]));
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    const DB_NAME: &str = "example_insert_test";
    const ROWS: usize = 100_000;
    const DEFAULT_RUNS: usize = 10;

    // Read any cli args
    let args = std::env::args().collect::<Vec<_>>()[1..].to_vec();

    // Pull out number of runs
    let num_runs = if args.is_empty() {
        std::env::var("EXAMPLE_RUNS")
            .ok()
            .and_then(|e| e.parse::<usize>().ok())
            .unwrap_or(DEFAULT_RUNS)
    } else {
        args[0].parse::<usize>().expect("Expected single argument, # of iterations")
    };

    // Init tracing
    init();

    // Setup container once
    let ch = get_or_create_container().await;

    // Setup clients
    let client = arrow_tests::setup_test_arrow_client(ch)
        .with_compression(CompressionMethod::LZ4)
        .build::<ArrowFormat>()
        .await?;

    // Setup database and table
    arrow_tests::setup_database(DB_NAME, &client).await?;

    let batches: Vec<RecordBatch> =
        (0..num_runs).map(|_| arrow_tests::create_test_batch(ROWS, false)).collect::<Vec<_>>();
    let table = arrow_tests::setup_table(&client, DB_NAME, &batches[0].schema()).await?;
    let query = format!("INSERT INTO {table} FORMAT Native");

    for (i, batch) in batches.into_iter().enumerate() {
        eprintln!(
            "
            --------
            Entering loop {i}
            --------
            "
        );

        let mut stream = client
            .insert(query.as_str(), batch, None)
            .await
            .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
            .unwrap();

        while let Some(result) = stream.next().await {
            result?;
        }

        eprintln!(
            "
            ========
            Finished loop {i}
            ========
            "
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let qid = Qid::new();
    let query = format!("SELECT * FROM {table} LIMIT 100");
    let mut stream = client.query(query, Some(qid)).await?;
    while let Some(maybe_batch) = stream.next().await {
        let batch = maybe_batch?;
        arrow::util::pretty::print_batches(&[batch])?;
    }

    // Shutdown container
    ch.shutdown().await.map_err(|e| Error::External(Box::new(e)))?;

    Ok(())
}
