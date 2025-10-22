#![expect(unused_crate_dependencies)]
#![allow(
    unused_results,
    clippy::uninlined_format_args,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::manual_is_multiple_of,
    clippy::unused_enumerate_index,
    clippy::manual_div_ceil,
    clippy::too_many_lines,
    clippy::unused_self,
    clippy::cloned_instead_of_copied
)]
mod common;

use std::time::Instant;

use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, arrow_tests};
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;
use common::scale_utils::{
    calculate_bytes_per_row, insert_concurrent, print_params_table, print_schema_config,
    print_schema_summary, setup_benchmark_client,
};
use futures_util::StreamExt;

const CONV_THRESHOLD: f64 = 0.05; // 5% improvement threshold for convergence

// Configuration to test
#[derive(Debug, Clone, Copy, PartialEq)]
struct Config {
    workers:    usize,
    batch_size: usize,
}

// Benchmark result for a configuration
#[derive(Debug, Clone)]
struct BenchmarkResult {
    config:          Config,
    #[allow(dead_code)]
    durations:       Vec<f64>, // All iteration times (seconds) - kept for debugging
    avg_throughput:  f64, // Average rows/sec
    best_throughput: f64, // Best rows/sec
    variance:        f64, // Coefficient of variation
}

impl BenchmarkResult {
    fn score(&self) -> f64 {
        // Score = best_throughput with penalty for high variance
        // Lower variance = more predictable = better for production
        self.best_throughput * (1.0 - self.variance.min(0.5))
    }
}

// SGD-like optimizer for finding optimal configuration
struct Optimizer {
    #[allow(dead_code)]
    total_rows: usize, // Kept for future heuristics
    history:    Vec<BenchmarkResult>,
    iteration:  usize,
}

impl Optimizer {
    fn new(total_rows: usize) -> Self { Self { total_rows, history: Vec::new(), iteration: 0 } }

    /// Initial guesses using heuristics
    fn initial_guesses(&self) -> Vec<Config> {
        vec![
            // Conservative: low workers, small batches
            Config { workers: 4, batch_size: 2_000 },
            // Balanced: medium everything
            Config { workers: 8, batch_size: 4_000 },
            // Aggressive: high workers, large batches
            Config { workers: 16, batch_size: 8_000 },
        ]
    }

    /// Generate next guesses, similar to gradient ascent
    fn next_guesses(&self) -> Vec<Config> {
        if self.history.is_empty() {
            return self.initial_guesses();
        }

        // Find best config so far
        let best =
            self.history.iter().max_by(|a, b| a.score().partial_cmp(&b.score()).unwrap()).unwrap();

        eprintln!(
            "  Current best: {:?} ({:.2}M rows/sec, variance: {:.1}%)",
            best.config,
            best.best_throughput / 1_000_000.0,
            best.variance * 100.0
        );

        // Explore around the best configuration
        let mut candidates = Vec::new();
        let base_w = best.config.workers;
        let base_b = best.config.batch_size;

        // Worker variations (±50%, but clamped to valid range)
        for w_mult in [0.5, 1.0, 2.0] {
            let workers = ((base_w as f64 * w_mult) as usize).clamp(4, 16);

            // Batch size variations (±50%, log scale)
            for b_mult in [0.5, 1.0, 2.0] {
                let batch_size = ((base_b as f64 * b_mult) as usize).clamp(1_000, 32_000);

                let config = Config { workers, batch_size };

                // Don't re-test configs we've already tested
                if !self.history.iter().any(|r| r.config == config) {
                    // Also don't add duplicates within candidates
                    if !candidates.contains(&config) {
                        candidates.push(config);
                    }
                }
            }
        }

        // If we've tested everything nearby, expand search
        if candidates.is_empty() {
            eprintln!("  No new candidates near best - expanding search space");
            candidates = vec![
                Config { workers: 4, batch_size: 16_000 },
                Config { workers: 8, batch_size: 16_000 },
                Config { workers: 12, batch_size: 8_000 },
                Config { workers: 16, batch_size: 16_000 },
                Config { workers: 16, batch_size: 32_000 },
            ]
            .into_iter()
            .filter(|c| !self.history.iter().any(|r| r.config == *c))
            .collect();
        }

        // Limit to top 3 candidates to keep iterations fast
        candidates.truncate(3);
        candidates
    }

    fn add_result(&mut self, result: BenchmarkResult) { self.history.push(result); }

    fn has_converged(&self, min_iterations: usize) -> bool {
        if self.iteration < min_iterations {
            return false;
        }

        // Check if best hasn't improved in last 2 iterations
        if self.history.len() < 6 {
            return false;
        }

        let recent: Vec<_> = self.history.iter().rev().take(6).collect();
        let best_recent = recent[0..3].iter().map(|r| r.score()).fold(f64::NEG_INFINITY, f64::max);
        let best_previous =
            recent[3..6].iter().map(|r| r.score()).fold(f64::NEG_INFINITY, f64::max);

        // Converged if improvement < 5%
        (best_recent - best_previous) / best_previous < CONV_THRESHOLD
    }

    fn get_best(&self) -> &BenchmarkResult {
        // Get the configuration with the highest raw throughput
        self.history
            .iter()
            .max_by(|a, b| a.best_throughput.partial_cmp(&b.best_throughput).unwrap())
            .expect("No results")
    }

    fn print_summary(&self, bytes_per_row: f64, config: &arrow_tests::BatchConfig) {
        eprintln!();
        common::print_banner(
            &format!("Tuning Summary - {} configurations tested", self.history.len()),
            Some(72),
        );

        let mut sorted = self.history.clone();
        sorted.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap());

        eprintln!("\nTop 5 configurations:");
        let mut table = Table::new();
        table.load_preset(UTF8_FULL).set_header(vec![
            "Workers",
            "Batch Size",
            "Best rows/sec",
            "Best MB/sec",
            "Variance",
            "Score",
        ]);

        for result in sorted.iter().take(5) {
            let mb_sec = result.best_throughput * bytes_per_row / 1_000_000.0;
            table.add_row(vec![
                result.config.workers.to_string(),
                result.config.batch_size.to_string(),
                format!("{:.0}", result.best_throughput),
                format!("{:.2}", mb_sec),
                format!("{:.1}%", result.variance * 100.0),
                format!("{:.0}", result.score()),
            ]);
        }

        eprintln!("{}", table);

        let best = self.get_best();
        let mb_sec = best.best_throughput * bytes_per_row / 1_000_000.0;
        let best_duration = best.durations.iter().cloned().fold(f64::INFINITY, f64::min);

        eprintln!();
        eprintln!("🏆 BEST RESULT:");
        eprintln!(
            "   Configuration: {} workers, {} batch size",
            best.config.workers, best.config.batch_size
        );
        eprintln!(
            "   Throughput:    {:.2}M rows/sec ({:.2} MB/sec)",
            best.best_throughput / 1_000_000.0,
            mb_sec
        );
        eprintln!("   Duration:      {:.3}s", best_duration);
        eprintln!("   Variance:      {:.1}%", best.variance * 100.0);

        // Display test schema
        eprintln!();
        eprintln!("📊 Test Schema:");
        print_schema_summary(config);
        eprintln!("  RecordBatch: {:.2} bytes/row", bytes_per_row);

        // Warning for variable-length types
        if config.utf8 > 0 || config.binary > 0 {
            eprintln!();
            eprintln!(
                "⚠️  Variable-length types detected (UTF8={}, BINARY={})",
                config.utf8, config.binary
            );
            eprintln!("   Bytes/row may vary with batch size due to Arrow overhead.");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::any::Any + Send>> {
    common::run_example_with_cleanup(|ch| async move { run(ch).await.unwrap() }, None).await?;
    Ok(())
}

async fn run(ch: &'static ClickHouseContainer) -> Result<()> {
    let start_time = Instant::now();

    // Parse parameters
    let total_rows: usize = std::env::var("ROWS")
        .unwrap_or_else(|_| "10000000".to_string())
        .parse()
        .unwrap_or(10_000_000);

    let max_steps: usize =
        std::env::var("STEPS").unwrap_or_else(|_| "5".to_string()).parse().unwrap_or(5);

    let runs_per_config: usize = std::env::var("ITERS")
        .or_else(|_| std::env::var("RUNS"))
        .unwrap_or_else(|_| "3".to_string())
        .parse()
        .unwrap_or(3);

    // Get schema configuration
    let batch_config = arrow_tests::BatchConfig::from_env();

    print_params_table("Dynamic Performance Tuner", &[
        ("Total Rows", format!("{}", total_rows)),
        ("Max Steps", format!("{} (optimizer iterations)", max_steps)),
        ("Iters per config", format!("{} (runs to average)", runs_per_config)),
    ]);
    eprintln!();

    // Display schema configuration
    print_schema_config(&batch_config);
    eprintln!();

    // Measure actual RecordBatch size
    let bytes_per_row = calculate_bytes_per_row(&batch_config);
    eprintln!("RecordBatch: {:.2} bytes/row", bytes_per_row);
    eprintln!();

    let mut optimizer = Optimizer::new(total_rows);
    let db = common::DB_NAME;

    // Setup database schema once
    let schema = arrow_tests::create_test_batch_with_config(1, &batch_config).schema();

    for step in 0..max_steps {
        optimizer.iteration = step;

        eprintln!();
        common::print_banner(
            &format!("Step {}/{} - Testing new configurations", step + 1, max_steps),
            Some(72),
        );
        eprintln!();

        let configs =
            if step == 0 { optimizer.initial_guesses() } else { optimizer.next_guesses() };

        if configs.is_empty() {
            eprintln!("No new configurations to test - converged!");
            break;
        }

        eprintln!("Testing {} configurations:", configs.len());
        for (i, config) in configs.iter().enumerate() {
            eprintln!("  {}. {} workers, {} batch size", i + 1, config.workers, config.batch_size);
        }
        eprintln!();

        // Benchmark each configuration
        for config in configs {
            eprintln!(
                "→ Testing: {} workers, {} batch size ({} runs)",
                config.workers, config.batch_size, runs_per_config
            );

            let result = benchmark_config(
                ch,
                db,
                &schema,
                config,
                total_rows,
                runs_per_config,
                &batch_config,
            )
            .await?;

            eprintln!(
                "  Result: Avg {:.2}M rows/sec, Best {:.2}M rows/sec, Variance {:.1}%",
                result.avg_throughput / 1_000_000.0,
                result.best_throughput / 1_000_000.0,
                result.variance * 100.0
            );
            eprintln!();

            optimizer.add_result(result);
        }

        // Check convergence
        if optimizer.has_converged(2) {
            eprintln!("✓ Converged - optimal configuration found!");
            break;
        }
    }

    optimizer.print_summary(bytes_per_row, &batch_config);

    let total_elapsed = start_time.elapsed();
    eprintln!("\nTotal run time: {:.3}s", total_elapsed.as_secs_f64());

    Ok(())
}

async fn benchmark_config(
    ch: &'static ClickHouseContainer,
    db: &str,
    schema: &arrow::datatypes::SchemaRef,
    config: Config,
    total_rows: usize,
    runs: usize,
    batch_config: &arrow_tests::BatchConfig,
) -> Result<BenchmarkResult> {
    // Setup client with specified worker count
    let client = setup_benchmark_client(ch, config.workers).await?;
    arrow_tests::setup_database(db, &client).await?;

    // Create fresh table
    let table = arrow_tests::setup_table(&client, db, schema).await?;

    let mut durations = Vec::with_capacity(runs);

    // Run benchmark multiple times
    for run in 0..runs {
        let start = Instant::now();
        insert_concurrent(
            client.clone(),
            table.clone(),
            total_rows,
            config.batch_size,
            config.workers,
            batch_config,
        )
        .await;
        let duration = start.elapsed().as_secs_f64();
        durations.push(duration);

        // Truncate for next run (except last)
        if run < runs - 1 {
            drop(
                client
                    .query(format!("TRUNCATE TABLE {table}"), None)
                    .await?
                    .collect::<Vec<_>>()
                    .await,
            );
        }
    }

    // Calculate statistics
    let avg_duration = durations.iter().sum::<f64>() / durations.len() as f64;
    let best_duration = durations.iter().cloned().fold(f64::INFINITY, f64::min);

    let avg_throughput = total_rows as f64 / avg_duration;
    let best_throughput = total_rows as f64 / best_duration;

    // Coefficient of variation
    let variance = if durations.len() > 1 {
        let mean = avg_duration;
        let variance_val = durations.iter().map(|d| (d - mean).powi(2)).sum::<f64>()
            / (durations.len() - 1) as f64;
        (variance_val.sqrt() / mean).abs()
    } else {
        0.0
    };

    // Cleanup
    drop(client.query(format!("DROP TABLE {table}"), None).await?.collect::<Vec<_>>().await);

    Ok(BenchmarkResult { config, durations, avg_throughput, best_throughput, variance })
}
