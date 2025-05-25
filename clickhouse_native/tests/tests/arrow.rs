use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use clickhouse_native::prelude::*;
use clickhouse_native::test_utils::ClickHouseContainer;
use clickhouse_native::{
    ArrowOptions, CompressionMethod, ConnectionStatus, CreateOptions, Result as ClickHouseResult,
    Type,
};
use futures_util::StreamExt;
use tracing::debug;

// assertions helpers for divergences from round trip precision
use crate::common::arrow_helpers::assertions::*;
use crate::common::arrow_helpers::*;
use crate::common::header;

/// Test arrow e2e using `ClientBuilder`.
///
/// NOTES:
/// 1. Strings as strings is used
/// 2. Date32 for Date is used.
/// 3. Strict schema's will be converted (when available).
///
/// # Panics
pub async fn test_round_trip(ch: &'static ClickHouseContainer) {
    let (client, options) = bootstrap(ch).await;

    // Create table with schema and enum mappings
    let schema = test_schema();

    // Create test RecordBatch
    let batch = test_record_batch();

    // Create schema
    let (db, table) =
        create_schema(&client, schema, &options).await.expect("Schema creation failed");

    // Round trip
    round_trip(&format!("{db}.{table}"), &client, batch).await.expect("Round trip failed");

    // Drop schema
    drop_schema(&db, &table, &client).await.expect("Drop table");

    client.shutdown().await.unwrap();
}

// Test arrow schema functions
/// # Panics
pub async fn test_schema_utils(ch: &'static ClickHouseContainer) {
    let (client, options) = bootstrap(ch).await;

    // Create table with schema and enum mappings
    let schema = test_schema();

    // Create schema
    let (db, table) = create_schema(&client, Arc::clone(&schema), &options)
        .await
        .expect("Schema creation failed");

    // Test fetch databases
    let query_id = Qid::new();
    header(query_id, "Fetching databases");
    let databases = client.fetch_schemas(Some(query_id)).await.expect("Fetch databases failed");
    assert!(databases.contains(&db));
    eprintln!("Databases: {databases:?}");

    // Test fetch all tables
    let query_id = Qid::new();
    header(query_id, "Fetching all tables");
    let tables = client.fetch_all_tables(Some(query_id)).await.expect("Fetch all tables failed");
    let db_tables = tables.get(&db);
    assert!(db_tables.is_some());
    let tables = db_tables.unwrap();
    assert!(tables.contains(&table));
    eprintln!("All tables: {tables:?}");

    // Test fetch tables
    let query_id = Qid::new();
    header(query_id, "Fetching db tables");
    let tables = client.fetch_tables(Some(&db), Some(query_id)).await.expect("Fetch tables failed");
    assert!(tables.contains(&table));
    eprintln!("Tables: {tables:?}");

    // Test fetch schema unfiltered
    let query_id = Qid::new();
    header(query_id, "Fetching db schema (non-filtered)");
    let tables =
        client.fetch_schema(Some(&db), &[], Some(query_id)).await.expect("Fetch schema failed");
    let table_schema = tables.get(&table);
    assert!(table_schema.is_some());
    let table_schema = table_schema.unwrap();
    compare_schemas(table_schema, &schema);
    eprintln!("Schema: {table_schema:?}");

    // Test fetch schema filtered
    let query_id = Qid::new();
    header(query_id, "Fetching db schema (filtered)");
    let tables = client
        .fetch_schema(Some(&db), &[&table], Some(query_id))
        .await
        .expect("Fetch schema filtered failed");
    let table_schema = tables.get(&table);
    assert!(table_schema.is_some());
    let table_schema = table_schema.unwrap();
    compare_schemas(table_schema, &schema);
    eprintln!("Table Schema: {table_schema:?}");

    // Drop schema
    drop_schema(&db, &table, &client).await.expect("Drop table");
}

/// # Panics
pub async fn test_execute_queries(ch: &'static ClickHouseContainer) {
    let (client, _) = bootstrap(ch).await;

    let settings_query = "SET allow_experimental_object_type = 1;";

    let query_id = Qid::new();
    header(query_id, "Settings query - execute");
    client
        .execute(settings_query, Some(query_id))
        .await
        .inspect_err(|error| error!(?error, "Failed to execute settings query"))
        .unwrap();

    let query_id = Qid::new();
    header(query_id, "Settings query - execute now");
    client
        .execute_now(settings_query, Some(query_id))
        .await
        .inspect_err(|error| error!(?error, "Failed to execute settings query now"))
        .unwrap();

    let query_id = Qid::new();
    header(query_id, "Simple scalar query");
    let query = "SELECT 1";
    let mut results = client
        .query(query, Some(query_id))
        .await
        .inspect_err(|error| error!(?error, "Failed to query simple scalar"))
        .unwrap();
    let response = results
        .next()
        .await
        .expect("Expected data from simple scalar")
        .expect("Expected no error for simple scalar");
    arrow::util::pretty::print_batches(&[response]).unwrap();

    client.shutdown().await.unwrap();
}

// Utility functions

pub(super) async fn bootstrap(ch: &'static ClickHouseContainer) -> (ArrowClient, CreateOptions) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    // Create ClientBuilder and ConnectionManager
    let client = Client::<ArrowFormat>::builder()
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

    client.health_check(true).await.expect("Health check failed");
    assert_eq!(client.status(), ConnectionStatus::Open);

    (client, options)
}

/// # Errors
/// # Panics
pub async fn create_schema(
    client: &ArrowClient,
    schema: SchemaRef,
    options: &CreateOptions,
) -> Result<(String, String), Box<dyn std::error::Error>> {
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

    Ok((db_name, table_name))
}

/// # Errors
pub async fn drop_schema(
    db: &str,
    table: &str,
    client: &ArrowClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let query_id = Qid::new();
    // Truncate table
    header(query_id, format!("Truncating table: {db}.{table}"));
    client.execute(format!("TRUNCATE TABLE {db}.{table}"), Some(query_id)).await?;

    // Drop table
    header(query_id, format!("Dropping table: {db}.{table}"));
    client.execute(format!("DROP TABLE {db}.{table}"), None).await?;

    // Drop database
    header(query_id, format!("Dropping database: {db}"));
    client.drop_database(db, true, None).await?;

    header(query_id, "Round-trip test completed successfully");

    Ok(())
}

/// # Errors
/// # Panics
pub async fn round_trip(
    table_ref: &str,
    client: &ArrowClient,
    batch: RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    let query_id = Qid::new();
    header(query_id, format!("Inserting RecordBatch with {} rows", batch.num_rows()));
    let query = format!("INSERT INTO {table_ref} FORMAT Native");
    let result = client
        .insert(&query, batch.clone(), Some(query_id))
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
    header(query_id, format!("Querying table: {table_ref}"));
    let query = format!("SELECT * FROM {table_ref}");
    let queried_batches = client
        .query(&query, Some(query_id))
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
        crate::roundtrip_exceptions!(
            (col.data_type(), inserted_column.data_type()) => {
                dict(k1, v1, _k2, _v2) => {{
                    assert_dictionaries(i, col, inserted_column, k1, v1);
                }};
                list(field1, field2) => {{
                    assert_lists(i, col, inserted_column, field1, field2);
                }};
                utc_default() => {{
                    assert_datetimes_utf_default(col, inserted_column);
                }};
            };
            _ => { assert_eq!(col, inserted_column, "Column {i} mismatch"); }
        );
    }

    Ok(())
}
