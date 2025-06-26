use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures_util::stream::StreamExt;

use super::utils::array_to_string_iter;
use crate::ArrowOptions;
use crate::prelude::*;

/// Fetches all tables for provided databases.
pub(crate) async fn fetch_tables(
    client: &ArrowClient,
    database: &str,
    qid: Option<Qid>,
) -> Result<Vec<String>> {
    let query = format!("SELECT name FROM system.tables WHERE database = '{database}'");
    let mut stream = client.query(query, qid).await?;
    let mut tables: Vec<String> = vec![];

    // Collect column metadata from the stream
    while let Some(batch) = stream.next().await.transpose()? {
        // 'name' as Utf8
        for value in array_to_string_iter(batch.column(0))?.flatten() {
            tables.push(value);
        }
    }

    Ok(tables)
}

/// Fetches all tables for all databases in a `ClickHouse` instance.
pub(crate) async fn fetch_all_tables(
    client: &Client<ArrowFormat>,
    qid: Option<Qid>,
) -> Result<HashMap<String, Vec<String>>> {
    let query = "SELECT database, name FROM system.tables WHERE database NOT IN ('system', \
                 'INFORMATION_SCHEMA')";
    let mut stream = client.query(query, qid).await?;
    let mut tables: HashMap<String, Vec<String>> = HashMap::new();

    // Collect column metadata from the stream
    while let Some(batch) = stream.next().await.transpose()? {
        // 'database' as Utf8
        let database_col = cast(batch.column(0), &DataType::Utf8)?;
        let database_col = database_col.as_string_opt::<i32>().ok_or(Error::ArrowDeserialize(
            "Could not deserialize table column for schema".into(),
        ))?;
        // 'name' as Utf8
        let name_col = cast(batch.column(1), &DataType::Utf8)?;
        let name_col = name_col.as_string_opt::<i32>().ok_or(Error::ArrowDeserialize(
            "Could not deserialize name column for schema".into(),
        ))?;
        for i in 0..batch.num_rows() {
            tables
                .entry(database_col.value(i).to_string())
                .or_default()
                .push(name_col.value(i).to_string());
        }
    }

    Ok(tables)
}

/// Fetches schemas for all tables in a `ClickHouse` database (or a subset if tables are specified).
pub(crate) async fn fetch_databases(
    client: &Client<ArrowFormat>,
    qid: Option<Qid>,
) -> Result<Vec<String>> {
    let query =
        "SELECT name FROM system.databases WHERE name NOT IN ('system', 'INFORMATION_SCHEMA')";
    let mut stream = client.query(query, qid).await?;
    let mut dbs: Vec<String> = vec![];

    // Collect column metadata from the stream
    while let Some(batch) = stream.next().await.transpose()? {
        // 'name' as Utf8
        for value in array_to_string_iter(batch.column(0))?.flatten() {
            dbs.push(value);
        }
    }

    Ok(dbs)
}

/// Fetches schemas for all tables in a `ClickHouse` database (or a subset if tables are specified).
pub(crate) async fn fetch_schema(
    client: &Client<ArrowFormat>,
    database: &str,
    tables: &[&str],
    qid: Option<Qid>,
    options: ArrowOptions,
) -> Result<HashMap<String, SchemaRef>> {
    let query = if tables.is_empty() {
        format!("SELECT table, name, type FROM system.columns WHERE database = '{database}'")
    } else {
        let table_list = tables
            .iter()
            .map(|t| format!("'{}'", t.trim_matches(['`', '\''])))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "SELECT table, name, type FROM system.columns WHERE database = '{database}' AND table \
             IN ({table_list})",
        )
    };

    let mut stream = client.query(query, qid).await?;
    let mut schemas: HashMap<String, Vec<Field>> = HashMap::new();

    // Collect column metadata from the stream
    while let Some(batch) = stream.next().await.transpose()? {
        // 'table' as Utf8
        let table_col = cast(batch.column(0), &DataType::Utf8)?;
        let table_col = table_col.as_string_opt::<i32>().ok_or(Error::ArrowDeserialize(
            "Could not deserialize table column for schema".into(),
        ))?;
        // 'name' as Utf8
        let name_col = cast(batch.column(1), &DataType::Utf8)?;
        let name_col = name_col.as_string_opt::<i32>().ok_or(Error::ArrowDeserialize(
            "Could not deserialize name column for schema".into(),
        ))?;
        // 'type' as Utf8
        let type_col = cast(batch.column(2), &DataType::Utf8)?;
        let type_col = type_col.as_string_opt::<i32>().ok_or(Error::ArrowDeserialize(
            "Could not deserialize type column for schema".into(),
        ))?;

        for i in 0..batch.num_rows() {
            let table = table_col.value(i).to_string();
            let name = name_col.value(i).to_string();
            let type_str = type_col.value(i).to_string();
            let ch_type = Type::from_str(&type_str)?;
            let (arrow_type, is_nullable) =
                super::types::ch_to_arrow_type(&ch_type, Some(options))?;
            let field = Field::new(name, arrow_type, is_nullable);
            schemas.entry(table).or_default().push(field);
        }
    }

    if schemas.is_empty() {
        return Ok(HashMap::default());
    }

    Ok(schemas
        .into_iter()
        .map(|(table, columns)| (table, Arc::new(Schema::new(columns))))
        .collect())
}
