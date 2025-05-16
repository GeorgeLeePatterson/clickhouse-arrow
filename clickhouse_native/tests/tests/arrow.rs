use std::collections::HashMap;

use arrow::array::*;
use arrow::datatypes::*;
use clickhouse_native::prelude::*;
use clickhouse_native::{
    ArrowOptions, ClientBuilder, CompressionMethod, CreateOptions, Result as ClickHouseResult, Type,
};
use futures_util::StreamExt;
use tracing::debug;

// assertions helpers for divergences from round trip precision
use crate::common::arrow_helpers::assertions::*;
use crate::common::arrow_helpers::*;
use crate::common::docker::ClickHouseContainer;
use crate::common::header;

/// # Panics
pub async fn test_round_trip(ch: &'static ClickHouseContainer) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    // Create ClientBuilder and ConnectionManager
    let client = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_compression(CompressionMethod::LZ4)
        .with_ipv4_only(true)
        // Use strings as strings to make sure that we are (de)serializing via strings
        .with_arrow_options(
            ArrowOptions::default()
                // Deserialize strings as Utf8, not Binary
                .with_strings_as_strings(true)
                // Deserialize Date as Date32
                .with_use_date32_for_date(true)
                // Ignore fields that ClickHouse doesn't support.
                .with_strict_schema_conversion(true),
        )
        .build()
        .await
        .expect("Building client");

    // Settings allows converting from "default" types that are compatible
    let schema_conversions = HashMap::from_iter([
        (
            "enum8_col".to_string(),
            Type::Enum8(vec![("active".to_string(), 0_i8), ("inactive".to_string(), 1)]),
        ),
        ("enum16_col".to_string(), Type::Enum16(vec![("x".to_string(), 0), ("y".to_string(), 1)])),
    ]);

    let options = CreateOptions::new("MergeTree")
        .with_order_by(&["id".to_string()])
        .with_schema_conversions(schema_conversions);

    // Create table with schema and enum mappings
    let schema = test_schema();

    // Create test RecordBatch
    let batch = test_record_batch();

    round_trip(client, schema, batch, &options).await.expect("Round trip failed");
}

/// # Errors
/// # Panics
pub async fn round_trip(
    client: ArrowClient,
    schema: SchemaRef,
    batch: RecordBatch,
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
    client.create_database(Some(&db_name), Some(table_qid)).await?;

    let query_id = Qid::new();
    header(query_id, format!("Creating table: {db_name}.{table_name}"));
    client
        .create_table(Some(&db_name), &table_name, &schema, options, Some(table_qid))
        .await?;

    let query_id = Qid::new();
    header(query_id, format!("Inserting RecordBatch with {} rows", batch.num_rows()));
    let query = format!("INSERT INTO {db_name}.{table_name} FORMAT Native");
    let result = client
        .insert(&query, batch.clone(), Some(table_qid))
        .await
        .inspect_err(|error| error!(?error, "Insertion failed: {query_id}"))?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()
        .inspect_err(|error| error!(?error, "Failed to insert RecordBatch: {query_id}"))?;
    drop(result);

    // Sleep wait for data
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Query and verify results
    let query_id = Qid::new();
    header(query_id, format!("Querying table: {db_name}.{table_name}"));
    let query = format!("SELECT * FROM {db_name}.{table_name}");
    let queried_batches = client
        .query(&query, Some(table_qid))
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()?;

    arrow::util::pretty::print_batches(&queried_batches)?;

    // Verify queried data matches inserted data
    header(query_id, "Verifying queried data");
    let inserted_batch = batch;

    assert_eq!(queried_batches.len(), 1, "Expected one batch");
    assert_eq!(queried_batches[0].num_rows(), inserted_batch.num_rows(), "Row count mismatch");

    for (i, col) in queried_batches[0].columns().iter().enumerate() {
        let inserted_column = inserted_batch.column(i);
        match (col.data_type(), inserted_column.data_type()) {
            // LowCardinality/Dictionaries
            (DataType::Dictionary(k1, v1), DataType::Dictionary(_, _)) => {
                assert_dictionaries(i, col, inserted_column, k1, v1);
            }
            // List LowCardinality/Dictionaries
            (
                DataType::List(field1) | DataType::LargeList(field1) | DataType::ListView(field1),
                DataType::List(field2) | DataType::LargeList(field2) | DataType::ListView(field2),
            ) => {
                assert_lists(i, col, inserted_column, field1, field2);
            }
            // Dates
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ) if tz.as_ref() == "UTC" => {
                assert_datetimes_utf_default(col, inserted_column);
            }
            // Default
            _ => assert_eq!(col, inserted_column, "Column {i} mismatch"),
        }
    }

    // Truncate table
    header(query_id, format!("Truncating table: {db_name}.{table_name}"));
    client.execute(format!("TRUNCATE TABLE {db_name}.{table_name}"), Some(table_qid)).await?;

    // Drop table
    header(query_id, format!("Dropping table: {db_name}.{table_name}"));
    client.execute(format!("DROP TABLE {db_name}.{table_name}"), None).await?;

    // Drop database
    header(query_id, format!("Dropping database: {db_name}"));
    client.drop_database(&db_name, true, None).await?;

    header(query_id, "Round-trip test completed successfully");

    Ok(())
}
