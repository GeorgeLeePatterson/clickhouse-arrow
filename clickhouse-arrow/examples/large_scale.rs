#![expect(unused_crate_dependencies)]
mod common;

use std::time::Instant;

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;
use common::scale_utils::{
    calculate_bytes_per_row, format_number, insert_concurrent, parse_number, print_schema_config,
    print_schema_summary, setup_benchmark_client,
};
use futures_util::StreamExt;

// Default configuration - override with env vars
// ROW_COUNTS: Comma-separated list, e.g., "1MM,5MM,10MM" or "1000000,5000000"
//   - M = thousands (×1,000), MM = millions (×1,000,000), K = thousands (×1,000)
// BATCH_SIZES: Comma-separated list of batch sizes, e.g., "5K,10K,20K" (default: 10K)
// WORKERS: Comma-separated list of worker counts, e.g., "8,16,32"
// ITERS: Iterations per test (default: 1)

#[derive(Debug, Clone)]
struct TestResult {
    workers:            usize,
    batch_size:         usize,
    rows:               usize,
    avg_duration_secs:  f64, // Outlier-stripped average
    best_duration_secs: f64, // Best (minimum) time
    avg_rows_per_sec:   f64, // Average throughput
    best_rows_per_sec:  f64, // Best (maximum) throughput
    count_time_secs:    f64,
    drop_time_secs:     f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::any::Any + Send>> {
    common::run_example_with_cleanup(|ch| async move { run(ch).await.unwrap() }, None).await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
async fn run(ch: &'static ClickHouseContainer) -> Result<()> {
    let total_start = Instant::now();
    let db = common::DB_NAME;

    // Parse comma-separated row counts (supports M/MM/K notation)
    let row_counts: Vec<usize> = std::env::var("ROW_COUNTS")
        .unwrap_or_else(|_| "1MM".to_string())
        .split(',')
        .filter_map(parse_number)
        .collect();

    // Parse comma-separated worker counts
    let worker_counts: Vec<usize> = std::env::var("WORKERS")
        .unwrap_or_else(|_| "16".to_string())
        .split(',')
        .filter_map(parse_number)
        .collect();

    // Parse comma-separated batch sizes (supports both BATCH_SIZES and BATCH_SIZE)
    let batch_sizes: Vec<usize> = std::env::var("BATCH_SIZES")
        .or_else(|_| std::env::var("BATCH_SIZE"))
        .unwrap_or_else(|_| "10K".to_string())
        .split(',')
        .filter_map(parse_number)
        .collect();

    let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(3).max(3); // Minimum 3 iterations for outlier stripping

    // Get schema configuration
    let config = arrow_tests::BatchConfig::from_env();

    common::print_banner("Large Scale Insert Test", Some(72));
    eprintln!();

    // Display schema configuration
    print_schema_config(&config);
    eprintln!();

    // Measure actual RecordBatch size (using 100K rows as representative sample)
    let bytes_per_row = calculate_bytes_per_row(&config);

    eprintln!("RecordBatch Memory Analysis (100K row sample):");
    eprintln!("  Bytes per row: {bytes_per_row:.2} bytes/row");
    eprintln!();

    eprintln!("Benchmark Parameters:");
    eprintln!("  Row counts:    {row_counts:?}");
    eprintln!("  Worker counts: {worker_counts:?}");
    eprintln!("  Batch sizes:   {batch_sizes:?}");
    eprintln!("  Iterations:    {iters} (outlier-stripped average)");
    eprintln!();

    let mut all_results = Vec::new();

    // Setup database
    let schema = arrow_tests::create_test_batch_generic(1).schema();

    // Run tests for each row count (outer loop)
    for total_rows in &row_counts {
        let header = format!("Testing {} rows", format_number(*total_rows));
        common::print_banner(&header, Some(72));
        eprintln!();

        // Run tests for each batch size
        for batch_size in &batch_sizes {
            eprintln!("  >> Batch size: {} <<", format_number(*batch_size));
            eprintln!();

            // Run tests for each worker count
            for workers in &worker_counts {
                let num_batches = (*total_rows).div_ceil(*batch_size);

                eprintln!(
                    "    --- {} workers ({} batches, {:.2} batches/worker) ---",
                    workers,
                    format_number(num_batches),
                    num_batches as f64 / *workers as f64
                );

                // Setup benchmark client with specified worker count
                let client = setup_benchmark_client(ch, *workers).await?;
                arrow_tests::setup_database(db, &client).await?;

                // Create fresh table for this test
                let table = arrow_tests::setup_table(&client, db, &schema).await?;

                let mut durations = Vec::with_capacity(iters);

                for iter in 1..=iters {
                    eprintln!("      Iteration {iter}/{iters}");

                    // Time the insert only
                    let start = Instant::now();
                    insert_concurrent(
                        client.clone(),
                        table.clone(),
                        *total_rows,
                        *batch_size,
                        *workers,
                        &config,
                    )
                    .await;
                    let duration = start.elapsed();
                    durations.push(duration);

                    eprintln!("        Duration: {:.3}s", duration.as_secs_f64());

                    // Truncate table for next iteration (not timed)
                    if iter < iters {
                        drop(
                            client
                                .query(format!("TRUNCATE TABLE {table}"), None)
                                .await?
                                .collect::<Vec<_>>()
                                .await,
                        );
                    }
                }

                // Calculate stats - strip min/max and average the rest
                let mut sorted_durations = durations.clone();
                sorted_durations.sort();

                // Strip outliers if we have 3+ iterations
                let trimmed_durations: Vec<std::time::Duration> = if sorted_durations.len() >= 3 {
                    sorted_durations[1..sorted_durations.len() - 1].to_vec()
                } else {
                    sorted_durations.clone()
                };

                let avg_duration = trimmed_durations.iter().sum::<std::time::Duration>()
                    / trimmed_durations.len() as u32;
                let best_duration = sorted_durations[0]; // Minimum time

                let avg_rows_per_sec = *total_rows as f64 / avg_duration.as_secs_f64();
                let best_rows_per_sec = *total_rows as f64 / best_duration.as_secs_f64();

                eprintln!(
                    "      Avg: {:.3}s | {} rows/sec | Best: {:.3}s | {} rows/sec",
                    avg_duration.as_secs_f64(),
                    format_number(avg_rows_per_sec as usize),
                    best_duration.as_secs_f64(),
                    format_number(best_rows_per_sec as usize)
                );

                // Time the count(*) verification
                let count_start = Instant::now();
                let count: u64 = client
                    .query(format!("SELECT count(*) FROM {table}"), None)
                    .await?
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?
                    .remove(0)
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .unwrap()
                    .value(0);
                let count_time = count_start.elapsed();

                assert_eq!(count, *total_rows as u64, "Row count mismatch!");

                // Time the drop operation
                let drop_start = Instant::now();
                drop(
                    client
                        .query(format!("DROP TABLE {table}"), None)
                        .await?
                        .collect::<Vec<_>>()
                        .await,
                );
                let drop_time = drop_start.elapsed();
                eprintln!();

                // Store result
                all_results.push(TestResult {
                    workers: *workers,
                    batch_size: *batch_size,
                    rows: *total_rows,
                    avg_duration_secs: avg_duration.as_secs_f64(),
                    best_duration_secs: best_duration.as_secs_f64(),
                    avg_rows_per_sec,
                    best_rows_per_sec,
                    count_time_secs: count_time.as_secs_f64(),
                    drop_time_secs: drop_time.as_secs_f64(),
                });
            }
        }
    }

    // Sort results by avg throughput (highest first)
    all_results.sort_by(|a, b| b.avg_rows_per_sec.partial_cmp(&a.avg_rows_per_sec).unwrap());

    // Print summary table
    eprintln!();
    eprintln!("SUMMARY RESULTS (sorted by avg throughput)");
    let mut table = Table::new();
    let _ = table.load_preset(UTF8_FULL).set_header(vec![
        "Workers",
        "Batch Size",
        "Rows",
        "Avg Time (s)",
        "Best Time(s)",
        "Avg rows/sec",
        "Best rows/sec",
        "Best MB/sec",
        "Count(s)",
        "Drop (s)",
    ]);

    for result in &all_results {
        let _ = table.add_row(vec![
            result.workers.to_string(),
            format_number(result.batch_size),
            format_number(result.rows),
            format!("{:.3}", result.avg_duration_secs),
            format!("{:.3}", result.best_duration_secs),
            format_number(result.avg_rows_per_sec as usize),
            format_number(result.best_rows_per_sec as usize),
            format!("{:.2}", result.best_rows_per_sec * bytes_per_row / 1_000_000.0),
            format!("{:.3}", result.count_time_secs),
            format!("{:.3}", result.drop_time_secs),
        ]);
    }

    eprintln!("{table}");

    // Calculate and show average count and drop times
    let avg_count_time =
        all_results.iter().map(|r| r.count_time_secs).sum::<f64>() / all_results.len() as f64;
    let avg_drop_time =
        all_results.iter().map(|r| r.drop_time_secs).sum::<f64>() / all_results.len() as f64;
    eprintln!();
    eprintln!("Average count(*) time: {avg_count_time:.3}s");
    eprintln!("Average drop time:     {avg_drop_time:.3}s");

    // Show best result
    let best = all_results
        .iter()
        .max_by(|a, b| a.best_rows_per_sec.partial_cmp(&b.best_rows_per_sec).unwrap())
        .unwrap();
    eprintln!();
    eprintln!("🏆 BEST RESULT:");
    eprintln!(
        "   Configuration: {} workers, {} batch size, {} rows",
        best.workers,
        format_number(best.batch_size),
        format_number(best.rows)
    );
    eprintln!(
        "   Throughput:    {:.2}M rows/sec ({:.2} MB/sec)",
        best.best_rows_per_sec / 1_000_000.0,
        best.best_rows_per_sec * bytes_per_row / 1_000_000.0
    );
    eprintln!("   Duration:      {:.3}s", best.best_duration_secs);

    // Display test schema
    eprintln!();
    eprintln!("📊 Test Schema:");
    print_schema_summary(&config);
    eprintln!("  RecordBatch: {bytes_per_row:.2} bytes/row");

    // Warning for variable-length types
    if config.utf8 > 0 || config.binary > 0 {
        eprintln!();
        eprintln!(
            "⚠️  Variable-length types detected (UTF8={}, BINARY={})",
            config.utf8, config.binary
        );
        eprintln!("   Bytes/row may vary with batch size due to Arrow overhead.");
    }

    // Show total run time
    let total_elapsed = total_start.elapsed();
    eprintln!("\nTotal run time: {:.3}s", total_elapsed.as_secs_f64());

    Ok(())
}
