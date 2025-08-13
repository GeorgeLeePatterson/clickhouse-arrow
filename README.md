# 🛰️ ClickHouse Arrow Client for Rust

## Crates

The project consists of two main crates:

### [`clickhouse-arrow`](./clickhouse-arrow/README.md)

The core client library that implements the `ClickHouse` native protocol with features like:

- Arrow integration across most types. For divergences, see the library's [README](./clickhouse-arrow/README.md)
- Ability to run both DDL and DML
- Native Rust macro for serde-like (de)serialization
- Performance in mind when working with data. If you have any suggestions for improving performance, I am always motivated to try and make it faster.

### [`clickhouse-arrow-derive`](./clickhouse-arrow-derive)

The `Row` macro. Procedural macros to (de)serialize rust data types into `ClickHouse` types:

- The `Row` macro for mapping Rust structs to `ClickHouse` tables for serde-like (de)serialization

For detailed documentation and examples, please refer to the library crate's README:

- [ClickHouse Arrow Client Documentation](./clickhouse-arrow/README.md)

## Example Usage

```rust
/// Basic connection example
use clickhouse_arrow::{ArrowFormat, Client, Result};
use clickhouse_arrow::arrow::arrow::util::pretty;
use futures_util::stream::StreamExt;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::<ArrowFormat>::builder()
        .with_url("http://localhost:9000")
        .with_database("default")
        .with_user("default")
        .build()?;

    // Query execution
    let batches = client
        .query("SELECT number FROM system.numbers LIMIT 10")
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    // Print RecordBatches
    pretty::print_record_batches(&batches)?;

    Ok(())
}
```

Refer to the e2e tests in [clickhouse-arrow](./clickhouse-arrow/tests/)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
clickhouse-arrow = "0.1.5"

# For derive macro support (on by default)
clickhouse-arrow = { version = "0.1", features = ["derive"] }

# For connection pooling
clickhouse-arrow = { version = "0.1", features = ["pool"] }
```

## Features

- **Native Protocol**: Direct communication with `ClickHouse`'s native protocol for optimal performance
- **Arrow Integration**: Seamless interoperability with the Arrow ecosystem
- **Flexible Types**: Support for both Arrow and native Rust types
- **Async First**: Built on modern async Rust for efficient I/O
- **Comprehensive**: Support for the full range of `ClickHouse` data types and features

## Performance

clickhouse-arrow is designed for high performance:

- **Zero-copy deserialization** where possible
- **Minimal allocations** during data transfer
- **Efficient streaming** for large datasets
- **Optimized type conversions** between Arrow and ClickHouse

Run benchmarks with:
```bash
cargo bench --features test-utils
```

## Supported Data Types

Full support for ClickHouse data types including:
- Numeric types (UInt8-256, Int8-256, Float32/64)
- String types (String, FixedString)
- Date/Time types (Date, Date32, DateTime, DateTime64)
- Complex types (Array, Nullable, LowCardinality, Map, Tuple)
- Special types (UUID, IPv4/IPv6, Enum8/16)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/GeorgeLeePatterson/clickhouse-arrow/blob/main/LICENSE) for details.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](https://github.com/GeorgeLeePatterson/clickhouse-arrow/blob/main/CONTRIBUTING.md) for guidelines.

## Releasing

Maintainer setup and release process:

### Setup
```bash
# Initialize development environment
just init-dev
```

### Making Releases
```bash
# Create a new release (patch/minor/major)
cargo release patch
```

Releases are fully automated and include:
- Changelog generation
- GitHub release creation
- Publishing to crates.io

## Acknowledgments

Special thanks to [klickhouse](https://github.com/Protryon/klickhouse), which provided inspiration and some initial code for this project to get started. While `clickhouse-arrow` Native has evolved into a complete rewrite in most areas, while others are essentially the same (`Row` macro), the early foundation benefited greatly from klickhouse's work. Ultimately the design goals are different, with this library focusing on Arrow interoperability and tools to make integrating `ClickHouse` and `Arrow` easier.
