use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use clickhouse_arrow::{Qid, Result as ClickHouseResult, Value};
use futures_util::StreamExt;
use tracing::error;

use super::arrow::{bootstrap, create_schema, drop_schema};
use crate::common::header;

/// Test arrow e2e using `ClientBuilder`.
///
/// NOTES:
/// 1. Strings as strings is used
/// 2. Date32 for Date is used.
/// 3. Strict schema's will be converted (when available).
///
/// # Panics
pub async fn test_arrow_compat(ch: Arc<ClickHouseContainer>) {
    let (client, options) = bootstrap(ch.as_ref(), None).await;

    // Test profile events
    let mut rx = client.subscribe_events();
    #[expect(clippy::disallowed_methods)]
    drop(tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            let client_id = event.client_id;
            let ev: &str = event.event.as_ref();
            println!("New profile event: client id = {client_id}, event = {ev}");
        }
    }));

    let ids = vec![0, 1, 2];
    let names = vec!["John", "Jane", "Mary"];

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int32Array::from(ids.clone())) as ArrayRef,
        Arc::new(StringArray::from(names.clone())) as ArrayRef,
    ])
    .unwrap();

    // Create schema
    let (db, table) =
        create_schema(&client, schema, &options).await.expect("Schema creation failed");
    let table_ref = format!("{db}.{table}");

    // Insert
    let query_id = Qid::new();
    header(query_id, format!("Inserting RecordBatch with {} rows", batch.num_rows()));
    let query = format!("INSERT INTO {table_ref} FORMAT Native");
    let result = client
        .insert(&query, batch.clone(), Some(query_id))
        .await
        .inspect_err(|error| error!(?error, "Insertion failed: {query_id}"))
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()
        .inspect_err(|error| error!(?error, "Failed to insert RecordBatch: {query_id}"))
        .unwrap();
    drop(result);

    // Sleep wait for data
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Query and verify results
    let query_id = Qid::new();
    header(query_id, format!("Querying table: {table_ref}"));
    let query = format!("SELECT * FROM {table_ref}");
    let queried_batches = client
        .query_rows(&query, Some(query_id))
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()
        .unwrap();
    assert!(queried_batches.len() == 3);

    for i in 0..queried_batches.len() {
        let row = &queried_batches[i];
        let id = &ids[i];
        let name = names[i].as_bytes();
        assert!(matches!(&row[0], Value::Int32(inner) if inner == id));
        assert!(matches!(&row[1], Value::String(inner) if inner.as_slice() == name));

        header(query_id, format!("Row {i} matches"));
    }

    // Drop schema
    drop_schema(&db, &table, &client).await.expect("Drop table");

    client.shutdown().await.unwrap();
}
