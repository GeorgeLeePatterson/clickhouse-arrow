#![expect(unused_crate_dependencies)]
mod common;

use clickhouse::{Client as ClickHouseRsClient, Row as ClickHouseRow};
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use uuid::Uuid;

#[derive(ClickHouseRow, Clone, Serialize, Deserialize)]
pub(crate) struct ClickHouseRsRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    i64, // DateTime64(3) maps to i64 milliseconds
}

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

#[allow(clippy::cast_precision_loss)]
async fn run(ch: &'static ClickHouseContainer, num_runs: usize) -> Result<()> {
    let db = common::DB_NAME;

    // Setup clients
    let client = arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
        .with_compression(CompressionMethod::LZ4)
        .build::<ArrowFormat>()
        .await?;

    // Setup database and table
    arrow_tests::setup_database(db, &client).await?;

    let rows = common::ROWS + 30_000;

    // Create RecordBatches
    // let batches =
    //     (0..num_runs).map(|_| arrow_tests::create_test_batch(rows, false)).collect::<Vec<_>>();
    let batch = arrow_tests::create_test_batch(rows, false);

    // Setup table
    let table = arrow_tests::setup_table(&client, db, &batch.schema()).await?;

    let mut arrow_runs = Vec::with_capacity(num_runs);
    let mut rs_runs = Vec::with_capacity(num_runs);

    // ---
    // ARROW
    // ---

    // Insert data
    let query = format!("INSERT INTO {table} FORMAT Native");
    common::header(format!("ARROW: Inserting {} rows", num_runs * rows));
    let start = Instant::now();

    // let mut stream = client
    //     .insert_many(query.as_str(), batches, None)
    //     .await
    //     .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
    //     .unwrap();
    // common::timing(format!("ARROW: Inserted {} rows", num_runs * rows), start);
    // while let Some(result) = stream.next().await {
    //     result?;
    // }

    for i in 0..num_runs {
        let b = batch.clone();
        let inner_start = Instant::now();
        let _stream = client
            .insert(query.as_str(), b, None)
            .await
            .inspect_err(|e| eprintln!("Insert error\n{e:?}"))
            .unwrap();
        // while let Some(result) = stream.next().await {
        //     result?;
        // }
        arrow_runs.push(inner_start.elapsed().as_secs_f64());
        common::timing(format!("ARROW: Inserted batch {i} - {rows} rows"), inner_start);
    }

    common::timing(format!("ARROW: Total Inserted {} rows", num_runs * rows), start);

    // ---
    // RS
    // ---

    let rs_client = ClickHouseRsClient::default()
        .with_url(ch.get_http_url())
        .with_user(&ch.user)
        .with_password(&ch.password)
        .with_database("default")
        .with_compression(clickhouse::Compression::Lz4);

    #[allow(clippy::cast_possible_wrap)]
    #[allow(clippy::cast_precision_loss)]
    let batches = (0..num_runs)
        .map(|run| {
            (0..rows)
                .map(|i| ClickHouseRsRow {
                    id:    Uuid::new_v4().to_string(),
                    name:  format!("name{i}"),
                    value: (run * i) as f64,
                    ts:    i as i64 * 1000,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let start = Instant::now();
    common::header(format!("CHRS: Inserting {} rows", num_runs * rows));
    for (i, batch) in batches.into_iter().enumerate() {
        let inner_start = Instant::now();
        let mut insert = rs_client.insert(&table).unwrap();
        for row in batch {
            insert.write(&row).await.unwrap();
        }
        insert.end().await.unwrap();
        rs_runs.push(inner_start.elapsed().as_secs_f64());
        common::timing(format!("CHRS: Inserted batch {i} - {rows} rows"), inner_start);
    }
    common::timing(format!("CHRS: Total Inserted {} rows", num_runs * rows), start);

    eprintln!("---------------");
    eprintln!("---- STATS ----");

    // Max/Min/Avg
    let arrow_max = arrow_runs.iter().copied().fold(0_f64, f64::max);
    let rs_max = rs_runs.iter().copied().fold(0_f64, f64::max);
    let arrow_min = arrow_runs.iter().copied().fold(1_000_f64, f64::min);
    let rs_min = rs_runs.iter().copied().fold(1_000_f64, f64::min);
    let arrow_avg = arrow_runs.iter().sum::<f64>() / arrow_runs.len() as f64;
    let rs_avg = rs_runs.iter().sum::<f64>() / rs_runs.len() as f64;

    eprintln!("Arrow:\n  Min: {arrow_min} - Avg: {arrow_avg} - Max: {arrow_max}");
    eprintln!("RS:\n  Min: {rs_min} - Avg: {rs_avg} - Max: {rs_max}");

    Ok(())
}
