# 🛰️ ClickHouse Native for Rust

This workspace provides a modern, efficient Rust client for ClickHouse using the native protocol with flexible data handling capabilities.

## Crates

The project consists of two main crates:

### [`clickhouse_native`](./clickhouse_native/README.md)

The core client library that implements the ClickHouse native protocol with features like:

- Arrow integration across most types. For divergences, see the library's [README](./clickhouse_native/README.md)
- Ability to run both DDL and DML
- Native Rust macro for serde-like (de)serialization
- Performance in mind when working with data. If you have any suggestions for improving performance, I am always motivated to try and make it faster. 

### [`clickhouse_native_derive`](./clickhouse_native_derive)

The `Row` macro. Procedural macros to (de)serialize rust data types into ClickHouse types:

- The `Row` macro for mapping Rust structs to ClickHouse tables for serde-like (de)serialization

For detailed documentation and examples, please refer to the library crate's README:

- [ClickHouse Native Client Documentation](./clickhouse_native/README.md)

## Example Usage

```rust
/// Basic connection example
use clickhouse_native::{ArrowFormat, ClientBuilder, Result};
use clickhouse_native::arrow::arrow::util::pretty;
use futures_util::stream::StreamExt;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new()
        .with_url("http://localhost:9000")
        .with_database("default")
        .with_user("default")
        .build::<ArrowFormat>()?;

    // Query execution
    let batches = client
        .query("SELECT number FROM system.numbers LIMIT 10")
        .await?
        .collect::<Vec<_>>()
        .await.into_iter()
        .collect::<Result<Vec<_>>>()?;

    // Print RecordBatches
    pretty::print_record_batches(&batches)?;

    Ok(())
}
```

Refer to the e2e tests in [clickhouse_native](./clickhouse_native/tests/e2e_arrow.rs)

## Features

- **Native Protocol**: Direct communication with ClickHouse for optimal performance
- **Arrow Integration**: Seamless interoperability with the Arrow ecosystem
- **Flexible Types**: Support for both Arrow and native Rust types
- **Async First**: Built on modern async Rust for efficient I/O
- **Comprehensive**: Support for the full range of ClickHouse data types and features

## License

Licensed under the terms of the LICENSE file included in the repository.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

Special thanks to [klickhouse](https://github.com/Protryon/klickhouse), which provided inspiration and some initial code for this project to get started. While ClickHouse Native has evolved into a complete rewrite in most areas, while others are essentially the same (`Row` macro), the early foundation benefited greatly from klickhouse's work. Ultimately the design goals are different, with this library focusing on Arrow interoperability and tools to make integrating ClickHouse and arrow easier.
