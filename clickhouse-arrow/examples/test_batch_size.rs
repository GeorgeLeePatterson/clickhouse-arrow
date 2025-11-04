#![expect(unused_crate_dependencies)]
#![allow(
    unused_results,
    clippy::uninlined_format_args,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::default_constructed_unit_structs,
    clippy::manual_string_new
)]

use clickhouse_arrow::test_utils::arrow_tests::{self, BatchConfig};
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;

fn main() {
    let config = BatchConfig::from_env();

    println!("╔════════════════════════════════════════════════════════════════════╗");
    println!("║  RecordBatch Size Analysis                                         ║");
    println!("╚════════════════════════════════════════════════════════════════════╝");
    println!();

    println!("Note: Use environment variables to configure schema.");
    println!("Examples:");
    println!("  INT32=4 FLOAT64=2 cargo run --example test_batch_size --features test-utils");
    println!("  UTF8=2 UTF8_LEN=20 cargo run --example test_batch_size --features test-utils");
    println!("  RAND=false cargo run --example test_batch_size --features test-utils");

    // Show configuration
    println!("Configuration:");
    println!(
        "  INT8={}, INT16={}, INT32={}, INT64={}",
        config.int8, config.int16, config.int32, config.int64
    );
    println!(
        "  UINT8={}, UINT16={}, UINT32={}, UINT64={}",
        config.uint8, config.uint16, config.uint32, config.uint64
    );
    println!("  FLOAT32={}, FLOAT64={}", config.float32, config.float64);
    println!("  BOOL={}", config.bool);
    println!("  UTF8={} (len={})", config.utf8, config.utf8_len);
    println!("  BINARY={} (len={})", config.binary, config.binary_len);
    println!("  TIMESTAMP={}", config.timestamp);
    println!("  RAND={}", config.rand);
    println!("  INCLUDE_ID={}", config.include_id);
    println!();

    // Warning for variable-length types
    if config.utf8 > 0 || config.binary > 0 {
        println!("⚠️  WARNING: Variable-length types detected!");
        println!("   - UTF8 columns: {}", config.utf8);
        println!("   - BINARY columns: {}", config.binary);
        println!();
        println!("   Variable-length types have non-uniform memory overhead:");
        println!("   • Small batches: High fixed overhead per row");
        println!("   • Large batches: Arrow may over-allocate capacity");
        println!("   • Bytes/row varies with batch size (see table above)");
        println!();
        println!("   For consistent, predictable memory usage, use fixed-size types:");
        println!("   INT32, INT64, FLOAT64, TIMESTAMP, etc.");
        println!();
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL).set_header(vec!["Rows", "Total bytes", "Bytes/row"]);

    let mut bytes_per_row_samples = Vec::new();

    for rows in [1_000, 10_000, 100_000, 1_000_000] {
        let batch = arrow_tests::create_test_batch_generic(rows);
        let size = batch.get_array_memory_size();
        let bytes_per_row = size as f64 / rows as f64;
        bytes_per_row_samples.push(bytes_per_row);

        table.add_row(vec![rows.to_string(), size.to_string(), format!("{:.2}", bytes_per_row)]);
    }

    // Calculate average bytes/row
    let avg_bytes_per_row =
        bytes_per_row_samples.iter().sum::<f64>() / bytes_per_row_samples.len() as f64;
    table.add_row(vec!["Average".to_string(), "".to_string(), format!("{:.2}", avg_bytes_per_row)]);

    println!("{}", table);
    println!();
}
