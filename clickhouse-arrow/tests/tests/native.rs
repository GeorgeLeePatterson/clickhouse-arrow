use std::sync::Arc;

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use clickhouse_arrow::{CompressionMethod, CreateOptions, Result as ClickHouseResult};
use futures_util::StreamExt;
use tracing::debug;

use crate::common::header;
use crate::common::native_helpers::*;

/// # Panics
pub async fn test_round_trip(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    // Table create options
    let options = CreateOptions::new("MergeTree").with_order_by(&["id".to_string()]);

    // Create ClientBuilder and ConnectionManager
    let client: NativeClient = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .with_compression(CompressionMethod::LZ4)
        .build()
        .await
        .expect("Building client");
    let test_data = generate_test_block();
    round_trip(client, test_data, &options)
        .await
        .inspect_err(|error| {
            error!("Round trip for Native failed: {error:?}");
        })
        .expect("Round trip failed");
}

/// # Errors
/// # Panics
pub async fn round_trip<T: Row + std::fmt::Debug + PartialEq + Clone + Send + Sync + 'static>(
    client: NativeClient,
    data: Vec<T>,
    options: &CreateOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    // Generate unique database and table names
    let table_qid = Qid::new();

    let db_name = format!("test_db_{table_qid}");
    let table_name = format!("test_table_{table_qid}");

    // Drop table
    let query_id = Qid::new();
    header(query_id, format!("Dropping table: {db_name}.{table_name}"));
    client
        .execute(format!("DROP TABLE IF EXISTS {db_name}.{table_name}"), Some(table_qid))
        .await?;

    // Drop database
    let query_id = Qid::new();
    header(query_id, format!("Dropping database: {db_name}"));
    client.execute(format!("DROP DATABASE IF EXISTS {db_name}"), Some(table_qid)).await?;

    // Create database
    let query_id = Qid::new();
    header(query_id, format!("Creating database: {db_name}"));
    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {db_name}"), Some(table_qid))
        .await?;

    // Create table
    let query_id = Qid::new();
    header(query_id, format!("Creating table: {db_name}.{table_name}"));
    client
        .create_table::<TestRowAll>(Some(&db_name), &table_name, options, Some(table_qid))
        .await?;

    // Insert data
    let query_id = Qid::new();
    header(query_id, format!("Inserting test data with {} rows", data.len()));
    let query = format!("INSERT INTO {db_name}.{table_name} FORMAT Native");
    let result = client
        .insert_rows(&query, data.clone().into_iter(), Some(table_qid))
        .await
        .inspect_err(|error| error!(?error, "Insertion failed: {query_id}"))?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()
        .inspect_err(|error| error!(?error, "Failed to insert rows: {query_id}"))?;
    drop(result);

    // Sleep wait for data
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Query and verify results
    let query_id = Qid::new();
    header(query_id, format!("Querying table: {db_name}.{table_name}"));
    let query = format!("SELECT * FROM {db_name}.{table_name}");
    let queried_rows = client
        .query(&query, Some(table_qid))
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<T>>>()?;

    eprintln!("Rows:\n{queried_rows:?}");

    // Verify queried data matches inserted data
    header(query_id, "Verifying queried data");
    let inserted_rows = data;

    assert_eq!(queried_rows.len(), inserted_rows.len(), "Expected equal rows");
    assert_eq!(queried_rows, inserted_rows, "Expected round trip of data");

    // Truncate table
    header(query_id, format!("Truncating table: {db_name}.{table_name}"));
    client.execute(format!("TRUNCATE TABLE {db_name}.{table_name}"), Some(table_qid)).await?;

    // Drop table
    header(query_id, format!("Dropping table: {db_name}.{table_name}"));
    client.execute(format!("DROP TABLE {db_name}.{table_name}"), None).await?;

    // Drop database
    header(query_id, format!("Dropping database: {db_name}"));
    client.execute(format!("DROP DATABASE {db_name}"), None).await?;

    header(query_id, "Round-trip test completed successfully");

    Ok(())
}
