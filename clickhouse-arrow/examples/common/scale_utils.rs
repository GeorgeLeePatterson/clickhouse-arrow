//! Utilities for large-scale benchmark examples
//!
//! This module provides shared functionality for performance testing,
//! including concurrent insert helpers, client setup, and formatting utilities.

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use clickhouse_arrow::test_utils::arrow_tests::{self, BatchConfig};
use futures_util::stream::{self, StreamExt};

/// Parse number with optional M/MM/K suffix
///
/// # Examples
/// - "1000" -> 1000
/// - "10K" -> 10,000
/// - "1M" -> 1,000
/// - "1MM" -> 1,000,000
#[allow(dead_code)] // Used by large_scale example
pub(crate) fn parse_number(s: &str) -> Option<usize> {
    let s = s.trim();

    if let Some(base) = s.strip_suffix("MM").or_else(|| s.strip_suffix("mm")) {
        base.trim().parse::<usize>().ok().map(|n| n * 1_000_000)
    } else if let Some(base) = s.strip_suffix("M").or_else(|| s.strip_suffix("m")) {
        base.trim().parse::<usize>().ok().map(|n| n * 1_000)
    } else if let Some(base) = s.strip_suffix("K").or_else(|| s.strip_suffix("k")) {
        base.trim().parse::<usize>().ok().map(|n| n * 1_000)
    } else {
        s.parse::<usize>().ok()
    }
}

/// Format number with comma separators for readability
///
/// # Example
/// ```
/// assert_eq!(format_number(1000), "1,000");
/// assert_eq!(format_number(1000000), "1,000,000");
/// ```
#[allow(dead_code)] // Used by large_scale example
pub(crate) fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, ch) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(*ch);
    }

    result
}

/// Calculate bytes per row for a given batch configuration
///
/// Uses a 100K row sample to estimate memory usage per row.
#[allow(clippy::cast_precision_loss)]
#[allow(dead_code)] // Used by large_scale and dynamic_tune examples
pub(crate) fn calculate_bytes_per_row(config: &BatchConfig) -> f64 {
    let test_batch = arrow_tests::create_test_batch_with_config(100_000, config);
    test_batch.get_array_memory_size() as f64 / 100_000.0
}

/// Setup a benchmark client with specified worker count
///
/// Configures client for performance testing:
/// - IPv4 only (faster connection)
/// - No compression (isolate insert performance)
/// - Connection pool sized for high concurrency (16 connections max)
///
/// Note: Pool size is set to 16 (max) to prevent connection exhaustion
/// during high-concurrency benchmarks, regardless of worker count.
#[allow(dead_code)] // Used by large_scale and dynamic_tune examples
pub(crate) async fn setup_benchmark_client(
    ch: &ClickHouseContainer,
    _workers: usize,
) -> Result<ArrowClient> {
    let mut client_builder =
        arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
            .with_ipv4_only(true)
            .with_compression(CompressionMethod::None);

    let mut options = client_builder.options().clone();
    // Use max pool size (16) for benchmarks to prevent connection exhaustion
    options.ext = options.ext.with_fast_mode_size(16);
    client_builder = client_builder.with_options(options);

    client_builder.build::<ArrowFormat>().await
}

/// Insert data concurrently across multiple batches
///
/// This function:
/// - Splits `total_rows` into batches of `batch_size`
/// - Generates unique IDs across batches if `config.unique_id` is true
/// - Inserts batches concurrently with specified worker count
/// - Handles the last batch being smaller than `batch_size`
#[allow(dead_code)] // Used by large_scale and dynamic_tune examples
pub(crate) async fn insert_concurrent(
    client: ArrowClient,
    table: String,
    total_rows: usize,
    batch_size: usize,
    concurrent: usize,
    config: &BatchConfig,
) {
    let query = format!("INSERT INTO {table} FORMAT NATIVE");
    let num_batches = total_rows.div_ceil(batch_size);
    let config = *config; // Copy config for move into async blocks

    let _results: Vec<_> = stream::iter(0..num_batches)
        .map(|batch_idx| {
            let q = query.clone();
            let c = client.clone();
            async move {
                // Calculate size for this batch - last batch may be smaller
                let rows_inserted_so_far = batch_idx * batch_size;
                let rows_remaining = total_rows.saturating_sub(rows_inserted_so_far);
                let this_batch_size = rows_remaining.min(batch_size);

                // Create batch with unique IDs if enabled
                let id_offset = if config.unique_id { Some(rows_inserted_so_far) } else { None };
                let batch = arrow_tests::create_test_batch_with_config_offset(
                    this_batch_size,
                    &config,
                    id_offset,
                );

                // Insert and consume the stream to ensure data is sent
                let mut stream = c
                    .insert(q.as_str(), batch, None)
                    .await
                    .inspect_err(|e| eprintln!("Insert error on batch {batch_idx}\n{e:?}"))
                    .unwrap();

                // Consume the stream to complete the insert
                while let Some(result) = stream.next().await {
                    result.unwrap();
                }
            }
        })
        .buffer_unordered(concurrent) // Keep N tasks running concurrently
        .collect()
        .await;
}

/// Print schema configuration, omitting fields with value 0
#[allow(unused)]
pub(crate) fn print_schema_config(config: &BatchConfig) {
    eprintln!("Schema Configuration:");

    // Always show boolean flags
    eprintln!("  INCLUDE_ID={}   (Int64 'id' column for ORDER BY)", config.include_id);
    eprintln!("  UNIQUE_ID={}    (unique IDs across batches)", config.unique_id);

    // Build list of non-zero integer fields
    let mut fields = Vec::new();
    if config.int8 > 0 {
        fields.push(format!("INT8={}", config.int8));
    }
    if config.int16 > 0 {
        fields.push(format!("INT16={}", config.int16));
    }
    if config.int32 > 0 {
        fields.push(format!("INT32={}", config.int32));
    }
    if config.int64 > 0 {
        fields.push(format!("INT64={}", config.int64));
    }
    if !fields.is_empty() {
        eprintln!("  {}", fields.join(", "));
    }

    // UINT types
    fields.clear();
    if config.uint8 > 0 {
        fields.push(format!("UINT8={}", config.uint8));
    }
    if config.uint16 > 0 {
        fields.push(format!("UINT16={}", config.uint16));
    }
    if config.uint32 > 0 {
        fields.push(format!("UINT32={}", config.uint32));
    }
    if config.uint64 > 0 {
        fields.push(format!("UINT64={}", config.uint64));
    }
    if !fields.is_empty() {
        eprintln!("  {}", fields.join(", "));
    }

    // Float types
    fields.clear();
    if config.float32 > 0 {
        fields.push(format!("FLOAT32={}", config.float32));
    }
    if config.float64 > 0 {
        fields.push(format!("FLOAT64={}", config.float64));
    }
    if !fields.is_empty() {
        eprintln!("  {}", fields.join(", "));
    }

    // Bool
    if config.bool > 0 {
        eprintln!("  BOOL={}", config.bool);
    }

    // UTF8
    if config.utf8 > 0 {
        eprintln!("  UTF8={} (len={})", config.utf8, config.utf8_len);
    }

    // Binary
    if config.binary > 0 {
        eprintln!("  BINARY={} (len={})", config.binary, config.binary_len);
    }

    // Timestamp
    if config.timestamp > 0 {
        eprintln!("  TIMESTAMP={}", config.timestamp);
    }

    eprintln!("  RAND={}   (random vs sequential data)", config.rand);
}

/// Print parameters in a formatted table
///
/// # Example
/// ```
/// print_params_table("Dynamic Performance Tuner", &[
///     ("Total Rows", "10000000"),
///     ("Max Steps", "4 (optimizer iterations)"),
///     ("Iters per config", "3 (runs to average)"),
/// ]);
/// ```
#[allow(unused)]
pub(crate) fn print_params_table(title: &str, params: &[(&str, String)]) {
    use comfy_table::presets::UTF8_FULL;
    use comfy_table::{Attribute, Cell, Table};

    let mut table = Table::new();
    let _ = table
        .load_preset(UTF8_FULL)
        .set_header(vec![Cell::new(title).add_attribute(Attribute::Bold)]);

    // Add each parameter as a row with key and value
    for (key, value) in params {
        let _ = table.add_row(vec![format!("{}:  {}", key, value)]);
    }

    eprintln!("{table}");
}

/// Print compact schema summary, showing only non-zero column types
#[allow(unused)]
pub(crate) fn print_schema_summary(config: &BatchConfig) {
    let mut fields = Vec::new();

    // Collect all non-zero columns
    if config.int8 > 0 {
        fields.push(format!("INT8={}", config.int8));
    }
    if config.int16 > 0 {
        fields.push(format!("INT16={}", config.int16));
    }
    if config.int32 > 0 {
        fields.push(format!("INT32={}", config.int32));
    }
    if config.int64 > 0 {
        fields.push(format!("INT64={}", config.int64));
    }
    if config.uint8 > 0 {
        fields.push(format!("UINT8={}", config.uint8));
    }
    if config.uint16 > 0 {
        fields.push(format!("UINT16={}", config.uint16));
    }
    if config.uint32 > 0 {
        fields.push(format!("UINT32={}", config.uint32));
    }
    if config.uint64 > 0 {
        fields.push(format!("UINT64={}", config.uint64));
    }
    if config.float32 > 0 {
        fields.push(format!("FLOAT32={}", config.float32));
    }
    if config.float64 > 0 {
        fields.push(format!("FLOAT64={}", config.float64));
    }
    if config.bool > 0 {
        fields.push(format!("BOOL={}", config.bool));
    }
    if config.utf8 > 0 {
        fields.push(format!("UTF8={}", config.utf8));
    }
    if config.binary > 0 {
        fields.push(format!("BINARY={}", config.binary));
    }
    if config.timestamp > 0 {
        fields.push(format!("TIMESTAMP={}", config.timestamp));
    }

    // Always show boolean flags
    fields.push(format!("INCLUDE_ID={}", config.include_id));
    fields.push(format!("UNIQUE_ID={}", config.unique_id));
    fields.push(format!("RAND={}", config.rand));

    eprintln!("  {}", fields.join(", "));
}
