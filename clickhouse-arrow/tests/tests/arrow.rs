use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use clickhouse_arrow::{
    ArrowOptions, CompressionMethod, ConnectionStatus, CreateOptions, Result as ClickHouseResult,
    Type,
};
use futures_util::StreamExt;
use tracing::debug;

// assertions helpers for divergences from round trip precision
use crate::common::arrow_helpers::assertions::*;
use crate::common::arrow_helpers::*;
use crate::common::header;

/// # Panics
pub async fn test_round_trip_none(ch: Arc<ClickHouseContainer>) {
    test_round_trip(ch, Some(CompressionMethod::None)).await;
}

/// # Panics
pub async fn test_round_trip_lz4(ch: Arc<ClickHouseContainer>) {
    test_round_trip(ch, Some(CompressionMethod::LZ4)).await;
}

/// # Panics
pub async fn test_round_trip_zstd(ch: Arc<ClickHouseContainer>) {
    test_round_trip(ch, Some(CompressionMethod::ZSTD)).await;
}

/// # Panics
pub async fn test_round_trip_none_large_data(ch: Arc<ClickHouseContainer>) {
    test_round_trip_large_data(ch, Some(CompressionMethod::None)).await;
}

/// # Panics
pub async fn test_round_trip_lz4_large_data(ch: Arc<ClickHouseContainer>) {
    test_round_trip_large_data(ch, Some(CompressionMethod::LZ4)).await;
}

/// # Panics
pub async fn test_round_trip_zstd_large_data(ch: Arc<ClickHouseContainer>) {
    test_round_trip_large_data(ch, Some(CompressionMethod::ZSTD)).await;
}

/// Test arrow e2e using `ClientBuilder`.
///
/// NOTES:
/// 1. Strings as strings is used
/// 2. Date32 for Date is used.
/// 3. Strict schema's will be converted (when available).
///
/// # Panics
pub async fn test_round_trip(ch: Arc<ClickHouseContainer>, compression: Option<CompressionMethod>) {
    let (client, options) = bootstrap(ch.as_ref(), compression).await;

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
    eprintln!("Client shutdown successfully");
}

/// # Panics
pub async fn test_round_trip_large_data(
    ch: Arc<ClickHouseContainer>,
    compression: Option<CompressionMethod>,
) {
    let (client, options) = bootstrap(ch.as_ref(), compression).await;

    // simple schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("datetime_col", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("string_col", DataType::Utf8, true),
    ]));

    // test batch with at least 65409 rows
    // and big enough for separate compression blocks
    let mut ids = Vec::new();
    let mut dts = Vec::new();
    let mut strings = Vec::new();
    for i in 0..65500 {
        ids.push(i);
        dts.push(Some(i64::from(i)));
        let s = format!("string_{}", i % 100);
        strings.push(Some(s));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int32Array::from(ids)),
        Arc::new(TimestampMillisecondArray::from(dts)),
        Arc::new(StringArray::from(strings)),
    ])
    .expect("Failed to create RecordBatch");

    // Create schema
    let (db, table) =
        create_schema(&client, schema, &options).await.expect("Schema creation failed");

    // Round trip
    round_trip(&format!("{db}.{table}"), &client, batch).await.expect("Round trip failed");

    // Drop schema
    drop_schema(&db, &table, &client).await.expect("Drop table");

    client.shutdown().await.unwrap();
    eprintln!("Client shutdown successfully");
}

// Test arrow schema functions
/// # Panics
pub async fn test_schema_utils(ch: Arc<ClickHouseContainer>) {
    let (client, options) = bootstrap(ch.as_ref(), None).await;

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
pub async fn test_execute_queries(ch: Arc<ClickHouseContainer>) {
    let (client, _) = bootstrap(ch.as_ref(), None).await;

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

/// Test named tuple field parsing (issue #85)
/// `ClickHouse` supports `Tuple(name1 Type1, name2 Type2)` syntax which was not being parsed
/// correctly.
///
/// # Panics
pub async fn test_named_tuple_schema(ch: Arc<ClickHouseContainer>) {
    let (client, _) = bootstrap(ch.as_ref(), None).await;

    // Create unique table name
    let table_qid = Qid::new();
    let table_name = format!("test_named_tuple_{table_qid}");

    // Create table with named tuple fields - this is the syntax that was failing
    let query_id = Qid::new();
    header(query_id, format!("Creating table with named tuple: {table_name}"));

    let create_table_query = format!(
        "CREATE TABLE {table_name} (
            id UInt32,
            simple_tuple Tuple(s String, i Int64),
            nested_tuple Tuple(name String, value Nullable(Int32), arr Array(String))
        ) ENGINE = Memory"
    );

    client.execute(&create_table_query, Some(query_id)).await.expect("Failed to create table");

    // Insert some data using ClickHouse SQL
    let query_id = Qid::new();
    header(query_id, format!("Inserting data into {table_name}"));
    let insert_query = format!(
        "INSERT INTO {table_name} VALUES
         (1, ('hello', 42), ('test', 100, ['a', 'b'])),
         (2, ('world', -1), ('example', NULL, ['x', 'y', 'z']))"
    );
    client.execute(&insert_query, Some(query_id)).await.expect("Failed to insert data");

    // Query the schema - this is where the TypeParseError was occurring
    let query_id = Qid::new();
    header(query_id, format!("Querying table schema: {table_name}"));
    let select_query = format!("SELECT * FROM {table_name} ORDER BY id");

    let queried_batches = client
        .query(&select_query, Some(query_id))
        .await
        .expect("Query failed - named tuple parsing may have failed")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()
        .expect("Failed to query data");

    arrow::util::pretty::print_batches(&queried_batches).unwrap();

    // Verify the data
    assert_eq!(queried_batches.len(), 1, "Expected one batch");
    let queried_batch = &queried_batches[0];
    assert_eq!(queried_batch.num_rows(), 2, "Expected 2 rows");
    assert_eq!(
        queried_batch.num_columns(),
        3,
        "Expected 3 columns (id, simple_tuple, nested_tuple)"
    );

    // Clean up
    let query_id = Qid::new();
    header(query_id, format!("Dropping table: {table_name}"));
    client
        .execute(format!("DROP TABLE {table_name}"), Some(query_id))
        .await
        .expect("Failed to drop table");

    client.shutdown().await.unwrap();
}

// Utility functions
pub(super) async fn bootstrap(
    ch: &ClickHouseContainer,
    compression: Option<CompressionMethod>,
) -> (ArrowClient, CreateOptions) {
    bootstrap_with_options(ch, compression, None::<fn(ClientBuilder) -> ClientBuilder>).await
}

pub(super) async fn bootstrap_with_options(
    ch: &ClickHouseContainer,
    compression: Option<CompressionMethod>,
    builder_options: Option<impl Fn(ClientBuilder) -> ClientBuilder>,
) -> (ArrowClient, CreateOptions) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    // Create ClientBuilder and ConnectionManager
    let builder = Client::<ArrowFormat>::builder()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_compression(compression.unwrap_or_default())
        .with_ipv4_only(true)
        // Use strings as strings to make sure that we are (de)serializing via strings
        .with_arrow_options(
            ArrowOptions::default()
                // Deserialize strings as Utf8, not Binary
                .with_strings_as_strings(true)
                // Deserialize Date as Date32
                .with_use_date32_for_date(true)
                // Ignore fields that ClickHouse doesn't support.
                .with_strict_schema(false)
                .with_nullable_array_default_empty(true)
                .with_disable_strict_schema_ddl(true),
        );

    let builder = if let Some(builder_options) = builder_options {
        builder_options(builder)
    } else {
        builder
    };

    let client = builder.build().await.expect("Building client");

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
    header(query_id, format!("Querying table (roundtrip select): {table_ref}"));
    let query = format!("SELECT * FROM {table_ref}");
    let queried_batches = client
        .query(&query, Some(query_id))
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()?;

    // Truncate results, they can be huge
    let truncated = queried_batches
        .clone()
        .into_iter()
        .map(|r| r.slice(0, 100.min(r.num_rows())))
        .collect::<Vec<_>>();
    arrow::util::pretty::print_batches(&truncated)?;

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

    // Test insert many
    // Create test RecordBatch
    let batches = (0..5).map(|_| test_record_batch()).collect::<Vec<_>>();
    let query = format!("INSERT INTO {table_ref} FORMAT Native");
    drop(
        client
            .insert_many(&query, batches, Some(query_id))
            .await
            .inspect_err(|error| error!(?error, "Insertion failed: {query_id}"))?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<ClickHouseResult<Vec<_>>>()
            .inspect_err(|error| error!(?error, "Failed to insert RecordBatches: {query_id}"))?,
    );

    let Some(column) =
        client.query_column("SELECT number FROM system.numbers_mt LIMIT 10", None).await?
    else {
        panic!("Failed to query column");
    };
    assert!(column.as_any().downcast_ref::<UInt64Array>().is_some());

    // Test query_one
    let query_id = Qid::new();
    header(query_id, "Testing query_one method");
    let Some(single_batch) =
        client.query_one("SELECT number FROM system.numbers_mt LIMIT 1", None).await?
    else {
        panic!("Failed to query_one");
    };
    assert_eq!(single_batch.num_rows(), 1, "query_one should return exactly one row");

    // Test query_one with no results
    let empty_result =
        client.query_one("SELECT number FROM system.numbers_mt LIMIT 0", None).await?;
    assert!(empty_result.is_none(), "query_one should return None for no results");

    Ok(())
}

/// Test `ClickHouse`'s actual support for various nullable array combinations
/// This will definitively tell us what `ClickHouse` supports vs what it rejects
/// # Panics
pub async fn test_clickhouse_nullable_array_support(ch: Arc<ClickHouseContainer>) {
    let (client, _) = bootstrap(ch.as_ref(), None).await;

    let base_table_name = format!("test_nullable_arrays_{}", Qid::new());

    // Test cases: (description, DDL, should_succeed)
    let test_cases = vec![
        // Basic array types that should work
        ("array_int", "Array(Int64)", true),
        ("array_nullable_int", "Array(Nullable(Int64))", true),
        ("array_array_int", "Array(Array(Int64))", true),
        ("array_array_nullable_int", "Array(Array(Nullable(Int64)))", true),
        // Nested arrays with nullable wrappers - should fail
        ("nullable_array_int", "Nullable(Array(Int64))", false),
        ("nullable_array_nullable_int", "Nullable(Array(Nullable(Int64)))", false),
        ("array_nullable_array_int", "Array(Nullable(Array(Int64)))", false),
        ("array_nullable_array_nullable_int", "Array(Nullable(Array(Nullable(Int64))))", false),
        ("nullable_array_array_int", "Nullable(Array(Array(Int64)))", false),
    ];

    for (field_name, ch_type, should_succeed) in test_cases {
        let table_name = format!("{base_table_name}_{field_name}");

        let create_query =
            format!("CREATE TABLE {table_name} (id UInt32, test_field {ch_type}) ENGINE = Memory");

        let query_id = Qid::new();
        header(query_id, format!("Testing {field_name}: {ch_type}"));

        let result = client.execute(&create_query, Some(query_id)).await;
        if should_succeed {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
    }

    client.shutdown().await.unwrap();
}

/// Test nullable array serialization to ensure no null mask is written for Array types
/// This reproduces the error: "Nested type Array(Nullable(Int64)) cannot be inside Nullable type"
///
/// # Panics
pub async fn test_nullable_array_serialization(ch: Arc<ClickHouseContainer>) {
    let (client, _) = bootstrap(ch.as_ref(), None).await;

    // Create a simple schema with nullable array field
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new(
            "nullable_array",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    ]));

    // Create test data with various null patterns
    let id_array = UInt32Array::from(vec![1, 2, 3, 4, 5]);

    // Create nullable array with nulls at different levels
    let mut builder = ListBuilder::new(Int64Builder::new());

    // Row 1: Non-null array with some null elements
    builder.values().append_value(10);
    builder.values().append_null();
    builder.values().append_value(30);
    builder.append(true);

    // Row 2: Null array (the array itself is null)
    builder.append(false);

    // Row 3: Non-null empty array
    builder.append(true);

    // Row 4: Non-null array with all null elements
    builder.values().append_null();
    builder.values().append_null();
    builder.append(true);

    // Row 5: Non-null array with non-null elements
    builder.values().append_value(100);
    builder.values().append_value(200);
    builder.append(true);

    let nullable_array = builder.finish();

    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(id_array),
        Arc::new(nullable_array),
    ])
    .expect("Failed to create RecordBatch");

    // Create unique table name
    let table_qid = Qid::new();
    let table_name = format!("test_nullable_array_{table_qid}");

    // Create table
    let query_id = Qid::new();
    header(query_id, format!("Creating table: {table_name}"));

    // ClickHouse doesn't support Nullable(Array), so we expect the type to be Array(Nullable(T))
    let create_table_query = format!(
        "CREATE TABLE {table_name} (
            id UInt32,
            nullable_array Array(Nullable(Int64))
        ) ENGINE = Memory"
    );

    client.execute(&create_table_query, Some(query_id)).await.expect("Failed to create table");

    // Test insertion - this is where the null mask issue would manifest
    let query_id = Qid::new();
    header(query_id, format!("Inserting RecordBatch into {table_name}"));
    let insert_query = format!("INSERT INTO {table_name} FORMAT Native");

    let result = client
        .insert(&insert_query, batch.clone(), Some(query_id))
        .await
        .expect("Insert query failed")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()
        .expect("Failed to insert RecordBatch");

    drop(result);

    // Query back the data to verify
    let query_id = Qid::new();
    header(query_id, format!("Querying table (nullable array serialization): {table_name}"));
    let select_query = format!("SELECT * FROM {table_name} ORDER BY id");

    let queried_batches = client
        .query(&select_query, Some(query_id))
        .await
        .expect("Query failed")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()
        .expect("Failed to query data");

    arrow::util::pretty::print_batches(&queried_batches).unwrap();

    // Verify the data
    assert_eq!(queried_batches.len(), 1, "Expected one batch");
    let queried_batch = &queried_batches[0];
    assert_eq!(queried_batch.num_rows(), 5, "Expected 5 rows");

    // Clean up
    let query_id = Qid::new();
    header(query_id, format!("Dropping table: {table_name}"));
    client
        .execute(format!("DROP TABLE {table_name}"), Some(query_id))
        .await
        .expect("Failed to drop table");

    client.shutdown().await.unwrap();
}
