# Examples

This directory contains examples demonstrating various features of clickhouse-arrow.

## Running Examples

All examples require the `test_utils` feature to be enabled:

```bash
# Run a specific example
cargo run --example select --features test_utils

# TODO: Remove - remove all "benchmark-like" stuff from examples
# Run with multiple iterations
EXAMPLE_RUNS=50 cargo run --example insert --features test_utils
```

## Available Examples

- **select.rs** - Basic query execution with Arrow format
- **insert.rs** - Inserting data using RecordBatches
- **insert_multi.rs** - Batch insertions with multiple RecordBatches
- **pool.rs** - Connection pooling with bb8
- **scalar.rs** - Working with scalar values and single rows

## Prerequisites

The examples will automatically start a ClickHouse container using testcontainers.
Make sure Docker is installed and running on your system.

## Environment Variables

> # TODO: Remove
- `EXAMPLE_RUNS` - Number of times to run the example (default: 10)
- `DISABLE_CLEANUP` - Set to `true` to keep the container running after the example
