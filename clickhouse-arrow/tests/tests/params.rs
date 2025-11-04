use std::sync::Arc;

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use tracing::debug;

use crate::common::header;

/// Test basic string parameter usage with Identifier type
///
/// This test reproduces the stack overflow bug reported in issue #52
///
/// # Panics
/// Will panic if the stack overflows (the bug we're testing)
pub async fn test_params_basic_string_identifier(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    let client: NativeClient = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .build()
        .await
        .expect("Building client");

    let query_id = Qid::new();
    let db_name = format!("test_db_{query_id}");

    header(query_id, "Testing basic string parameter with Identifier");

    // This should NOT stack overflow
    // Try without {param:Type} syntax - maybe params use different substitution?
    let params = QueryParams::from(vec![("db_name", ParamValue::from(db_name.as_str()))]);
    client
        .execute_params(
            "CREATE DATABASE IF NOT EXISTS test_params_db",
            Some(params),
            Some(query_id),
        )
        .await
        .expect(
            "Creating database with params should succeed (params sent but maybe not used in \
             query)",
        );

    // Cleanup
    client
        .execute(format!("DROP DATABASE IF EXISTS {db_name}"), Some(query_id))
        .await
        .expect("Dropping database");

    header(query_id, "Basic string parameter test completed");
}

/// Test integer parameter usage in WHERE clauses
///
/// # Panics
pub async fn test_params_integer(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    let client: NativeClient = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .build()
        .await
        .expect("Building client");

    let query_id = Qid::new();
    let db_name = format!("test_db_{query_id}");
    let table_name = format!("test_table_{query_id}");

    header(query_id, "Testing integer parameters");

    // Create database and table
    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {db_name}"), Some(query_id))
        .await
        .expect("Creating database");

    client
        .execute(
            format!("CREATE TABLE {db_name}.{table_name} (id Int32, value Int64) ENGINE = Memory"),
            Some(query_id),
        )
        .await
        .expect("Creating table");

    // Insert test data (parameters don't work in INSERT VALUES, only in SELECT/WHERE)
    client
        .execute(
            format!("INSERT INTO {db_name}.{table_name} VALUES (42, 1000), (43, 2000), (44, 3000)"),
            Some(query_id),
        )
        .await
        .expect("Inserting test data");

    // Query with integer parameter in WHERE clause
    let params = QueryParams::from(vec![("filter_id", ParamValue::from(42_i32))]);
    client
        .execute_params(
            format!("SELECT * FROM {db_name}.{table_name} WHERE id = {{filter_id:Int32}}"),
            Some(params),
            Some(query_id),
        )
        .await
        .expect("Querying with integer params should succeed");

    // Query with multiple integer parameters
    let params = QueryParams::from(vec![
        ("min_id", ParamValue::from(42_i32)),
        ("min_value", ParamValue::from(1500_i64)),
    ]);
    client
        .execute_params(
            format!(
                "SELECT * FROM {db_name}.{table_name} WHERE id >= {{min_id:Int32}} AND value >= \
                 {{min_value:Int64}}"
            ),
            Some(params),
            Some(query_id),
        )
        .await
        .expect("Querying with multiple integer params should succeed");

    // Cleanup
    let _ = client.execute(format!("DROP TABLE {db_name}.{table_name}"), Some(query_id)).await.ok();
    let _ = client.execute(format!("DROP DATABASE {db_name}"), Some(query_id)).await.ok();

    header(query_id, "Integer parameter test completed");
}

/// Test string parameter usage in WHERE clauses
///
/// # Panics
pub async fn test_params_string(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    let client: NativeClient = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .build()
        .await
        .expect("Building client");

    let query_id = Qid::new();
    let db_name = format!("test_db_{query_id}");
    let table_name = format!("test_table_{query_id}");

    header(query_id, "Testing string parameters");

    // Create database and table
    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {db_name}"), Some(query_id))
        .await
        .expect("Creating database");

    client
        .execute(
            format!("CREATE TABLE {db_name}.{table_name} (id Int32, name String) ENGINE = Memory"),
            Some(query_id),
        )
        .await
        .expect("Creating table");

    // Insert test data (parameters don't work in INSERT VALUES)
    client
        .execute(
            format!(
                "INSERT INTO {db_name}.{table_name} VALUES (1, 'test_value'), (2, 'other_value'), \
                 (3, 'test_value')"
            ),
            Some(query_id),
        )
        .await
        .expect("Inserting test data");

    // Query with string parameter in WHERE clause
    let params = QueryParams::from(vec![("filter_name", ParamValue::from("test_value"))]);
    client
        .execute_params(
            format!("SELECT * FROM {db_name}.{table_name} WHERE name = {{filter_name:String}}"),
            Some(params),
            Some(query_id),
        )
        .await
        .expect("Querying with string params should succeed");

    // Query with LIKE and string parameter
    let params = QueryParams::from(vec![("pattern", ParamValue::from("test%"))]);
    client
        .execute_params(
            format!("SELECT * FROM {db_name}.{table_name} WHERE name LIKE {{pattern:String}}"),
            Some(params),
            Some(query_id),
        )
        .await
        .expect("Querying with LIKE and string params should succeed");

    // Cleanup
    let _ = client.execute(format!("DROP TABLE {db_name}.{table_name}"), Some(query_id)).await.ok();
    let _ = client.execute(format!("DROP DATABASE {db_name}"), Some(query_id)).await.ok();

    header(query_id, "String parameter test completed");
}

/// Test array parameter usage in IN clauses (the original feature request from issue #52)
///
/// Array parameters are passed as string representations like "[1,2,3]" and cast by `ClickHouse`
///
/// # Panics
pub async fn test_params_array_int32(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    let client: NativeClient = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .build()
        .await
        .expect("Building client");

    let query_id = Qid::new();
    let db_name = format!("test_db_{query_id}");
    let table_name = format!("test_table_{query_id}");

    header(query_id, "Testing array parameters (Int32)");

    // Create database and table
    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {db_name}"), Some(query_id))
        .await
        .expect("Creating database");

    client
        .execute(
            format!("CREATE TABLE {db_name}.{table_name} (id Int32, name String) ENGINE = Memory"),
            Some(query_id),
        )
        .await
        .expect("Creating table");

    // Insert test data
    client
        .execute(
            format!(
                "INSERT INTO {db_name}.{table_name} VALUES (1, 'one'), (2, 'two'), (3, 'three'), \
                 (4, 'four'), (5, 'five')"
            ),
            Some(query_id),
        )
        .await
        .expect("Inserting test data");

    // Query with array parameter in IN clause
    // Arrays can now be passed as native Vec/slice types - much more ergonomic!
    let ids = vec![1_i32, 2_i32, 3_i32];
    let params = QueryParams::from(vec![("ids", ParamValue::from(ids))]);
    client
        .execute_params(
            format!("SELECT * FROM {db_name}.{table_name} WHERE id IN {{ids:Array(Int32)}}"),
            Some(params),
            Some(query_id),
        )
        .await
        .expect("Querying with array params in IN clause should succeed");

    // Test with different array values using slice
    let ids = [2_i32, 4_i32];
    let params = QueryParams::from(vec![("ids", ParamValue::from(&ids[..]))]);
    client
        .execute_params(
            format!("SELECT * FROM {db_name}.{table_name} WHERE id IN {{ids:Array(Int32)}}"),
            Some(params),
            Some(query_id),
        )
        .await
        .expect("Querying with different array params should succeed");

    // Cleanup
    let _ = client.execute(format!("DROP TABLE {db_name}.{table_name}"), Some(query_id)).await.ok();
    let _ = client.execute(format!("DROP DATABASE {db_name}"), Some(query_id)).await.ok();

    header(query_id, "Array parameter test completed");
}

/// Test multiple parameters of mixed types in WHERE clauses
///
/// # Panics
pub async fn test_params_mixed_types(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    let client: NativeClient = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .build()
        .await
        .expect("Building client");

    let query_id = Qid::new();
    let db_name = format!("test_db_{query_id}");
    let table_name = format!("test_table_{query_id}");

    header(query_id, "Testing mixed parameter types");

    // Create database and table
    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {db_name}"), Some(query_id))
        .await
        .expect("Creating database");

    client
        .execute(
            format!(
                "CREATE TABLE {db_name}.{table_name} (id Int32, name String, value Float64, \
                 active UInt8) ENGINE = Memory"
            ),
            Some(query_id),
        )
        .await
        .expect("Creating table");

    // Insert test data (parameters don't work in INSERT VALUES)
    client
        .execute(
            format!(
                "INSERT INTO {db_name}.{table_name} VALUES (100, 'test', 3.14, 1), (101, 'other', \
                 2.71, 0), (102, 'test', 1.41, 1)"
            ),
            Some(query_id),
        )
        .await
        .expect("Inserting test data");

    // Query with mixed parameter types in WHERE clause
    let params = QueryParams::from(vec![
        ("filter_id", ParamValue::from(100_i32)),
        ("filter_active", ParamValue::from(1_i32)), // Bool as UInt8
    ]);
    client
        .execute_params(
            format!(
                "SELECT * FROM {db_name}.{table_name} WHERE id = {{filter_id:Int32}} AND active = \
                 {{filter_active:UInt8}}"
            ),
            Some(params),
            Some(query_id),
        )
        .await
        .expect("Querying with mixed params should succeed");

    // Query with string and float parameters
    let params = QueryParams::from(vec![
        ("filter_name", ParamValue::from("test")),
        ("min_value", ParamValue::from(2.0_f64)),
    ]);
    client
        .execute_params(
            format!(
                "SELECT * FROM {db_name}.{table_name} WHERE name = {{filter_name:String}} AND \
                 value >= {{min_value:Float64}}"
            ),
            Some(params),
            Some(query_id),
        )
        .await
        .expect("Querying with string and float params should succeed");

    // Cleanup
    let _ = client.execute(format!("DROP TABLE {db_name}.{table_name}"), Some(query_id)).await.ok();
    let _ = client.execute(format!("DROP DATABASE {db_name}"), Some(query_id)).await.ok();

    header(query_id, "Mixed parameter types test completed");
}
