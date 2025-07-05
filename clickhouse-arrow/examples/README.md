# Examples

This directory contains examples demonstrating various features of clickhouse-arrow.

## Running Examples

All examples require the `test-utils` feature to be enabled:

```bash
# Run a specific example
cargo run --example insert --features test-utils
# or `just example insert`
```

## Available Examples

- **insert.rs** - Inserting Arrow RecordBatches
- **insert_multi.rs** - Inserting over spawned tokio tasks
- **pool.rs** - Connection pooling with bb8
- **scalar.rs** - Working with scalar values and single column, useful benchmark

## Prerequisites

The examples will automatically start a ClickHouse container using testcontainers.
Make sure Docker is installed and running on your system.
