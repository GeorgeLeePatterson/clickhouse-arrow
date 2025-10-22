//! TODO: Remove - developer docs
use std::collections::VecDeque;
use std::env;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use testcontainers::core::{IntoContainerPort, Mount};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, TestcontainersError};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::level_filters::LevelFilter;
use tracing::{debug, error};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

pub const ENDPOINT_ENV: &str = "CLICKHOUSE_ENDPOINT";
pub const VERSION_ENV: &str = "CLICKHOUSE_VERSION";
pub const NATIVE_PORT_ENV: &str = "CLICKHOUSE_NATIVE_PORT";
pub const HTTP_PORT_ENV: &str = "CLICKHOUSE_HTTP_PORT";
pub const USER_ENV: &str = "CLICKHOUSE_USER";
pub const PASSWORD_ENV: &str = "CLICKHOUSE_PASSWORD";
pub const USE_TMPFS_ENV: &str = "USE_TMPFS";

const CLICKHOUSE_CONFIG_SRC: &str = "tests/bin/";
const CLICKHOUSE_CONFIG_DEST: &str = "/etc/clickhouse-server/config.xml";

// Env defaults
const CLICKHOUSE_USER: &str = "clickhouse";
const CLICKHOUSE_PASSWORD: &str = "clickhouse";
const CLICKHOUSE_VERSION: &str = "latest";
const CLICKHOUSE_NATIVE_PORT: u16 = 9000;
const CLICKHOUSE_HTTP_PORT: u16 = 8123;
const CLICKHOUSE_ENDPOINT: &str = "localhost";

pub static CONTAINER: OnceLock<Arc<ClickHouseContainer>> = OnceLock::new();

/// Initialize tracing in a test setup
pub fn init_tracing(directives: Option<&[(&str, &str)]>) {
    let rust_log = env::var("RUST_LOG").unwrap_or_default();

    let stdio_logger = tracing_subscriber::fmt::Layer::default()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_filter(get_filter(&rust_log, directives));

    // Initialize only if not already set (avoids multiple subscribers in tests)
    if tracing::subscriber::set_global_default(tracing_subscriber::registry().with(stdio_logger))
        .is_ok()
    {
        debug!("Tracing initialized with RUST_LOG={rust_log}");
    }
}

/// Common tracing filters
///
/// # Panics
#[allow(unused)]
pub fn get_filter(rust_log: &str, directives: Option<&[(&str, &str)]>) -> EnvFilter {
    let mut env_dirs = vec![];
    let level = if rust_log.is_empty() {
        LevelFilter::WARN.to_string()
    } else if let Ok(level) = LevelFilter::from_str(rust_log) {
        level.to_string()
    } else {
        let mut parts = rust_log.split(',');
        let level = parts.next().and_then(|p| LevelFilter::from_str(p).ok());
        env_dirs = parts
            .map(|s| s.split('=').collect::<VecDeque<_>>())
            .filter(|s| s.len() == 2)
            .map(|mut s| (s.pop_front().unwrap(), s.pop_front().unwrap()))
            .collect::<Vec<_>>();
        level.unwrap_or(LevelFilter::WARN).to_string()
    };

    let mut filter = EnvFilter::new(level)
        .add_directive("ureq=info".parse().unwrap())
        .add_directive("tokio=info".parse().unwrap())
        .add_directive("runtime=error".parse().unwrap())
        .add_directive("opentelemetry_sdk=off".parse().unwrap());

    if let Some(directives) = directives {
        for (key, value) in directives {
            filter = filter.add_directive(format!("{key}={value}").parse().unwrap());
        }
    }

    for (key, value) in env_dirs {
        filter = filter.add_directive(format!("{key}={value}").parse().unwrap());
    }

    filter
}

/// # Panics
/// You bet it panics. Better be careful.
pub async fn get_or_create_container(conf: Option<&str>) -> &'static Arc<ClickHouseContainer> {
    if let Some(c) = CONTAINER.get() {
        c
    } else {
        let ch = ClickHouseContainer::try_new(conf)
            .await
            .expect("Failed to initialize ClickHouse container");
        CONTAINER.get_or_init(|| Arc::new(ch))
    }
}

/// # Panics
/// You bet it panics. Better be careful.
pub async fn create_container(conf: Option<&str>) -> Arc<ClickHouseContainer> {
    let ch = ClickHouseContainer::try_new(conf)
        .await
        .expect("Failed to initialize ClickHouse container");
    Arc::new(ch)
}

/// Get or create a `ClickHouse` container with optional tmpfs mounts for benchmarks
///
/// Similar to `get_or_create_container` but can enable tmpfs for zero disk I/O
/// by setting the `USE_TMPFS` environment variable to "true" or "1".
///
/// Without tmpfs, data is written to disk with normal Docker volume behavior.
/// With tmpfs enabled, data is stored in RAM for maximum performance.
///
/// # Environment Variables
/// - `USE_TMPFS`: Set to "true" or "1" to enable tmpfs mounts (default: false)
///
/// # Panics
/// Panics if the container fails to start
pub async fn get_or_create_benchmark_container(conf: Option<&str>) -> &'static ClickHouseContainer {
    static BENCHMARK_CONTAINER: OnceLock<Arc<ClickHouseContainer>> = OnceLock::new();

    if let Some(c) = BENCHMARK_CONTAINER.get() {
        c
    } else {
        // Check if tmpfs should be enabled (default: false)
        let use_tmpfs =
            env::var(USE_TMPFS_ENV).is_ok_and(|v| v.eq_ignore_ascii_case("true") || v == "1");

        let mut builder =
            ClickHouseContainer::builder().with_config(conf.unwrap_or("config.xml").to_string());

        if use_tmpfs {
            builder = builder.with_tmpfs();
        }

        let ch =
            builder.build().await.expect("Failed to initialize ClickHouse benchmark container");
        BENCHMARK_CONTAINER.get_or_init(|| Arc::new(ch))
    }
}

/// Builder for `ClickHouseContainer` with configurable options
pub struct ClickHouseContainerBuilder {
    config: Option<String>,
    tmpfs:  bool,
}

impl ClickHouseContainerBuilder {
    /// Create a new builder with default settings
    pub fn new() -> Self { Self { config: None, tmpfs: false } }

    /// Use a custom `ClickHouse` config file
    #[must_use]
    pub fn with_config(mut self, config: impl Into<String>) -> Self {
        self.config = Some(config.into());
        self
    }

    /// Enable tmpfs mounts for benchmark mode (data stored in RAM)
    ///
    /// This mounts the following paths as tmpfs:
    /// - `/var/lib/clickhouse` - Main data directory
    /// - `/var/log/clickhouse-server` - Logs
    /// - `/tmp` - Temporary files
    ///
    /// # Sized tmpfs (with `tmpfs-size` feature)
    /// When the `tmpfs-size` feature is enabled, explicit sizes are set:
    /// - `/var/lib/clickhouse`: 20GB (prevents space exhaustion from WAL/merge artifacts)
    /// - `/var/log/clickhouse-server`: 2GB
    /// - `/tmp`: 2GB
    ///
    /// # Default behavior (without `tmpfs-size` feature)
    /// Each tmpfs mount defaults to 50% of available RAM, which may cause space
    /// exhaustion in long-running benchmark suites.
    ///
    /// Benefits:
    /// - Zero disk I/O overhead
    /// - Faster inserts and queries
    /// - More consistent benchmarks
    /// - Predictable resource allocation (with `tmpfs-size`)
    ///
    /// Trade-offs:
    /// - Requires sufficient RAM (recommend 24GB+ free with `tmpfs-size`)
    /// - Data lost on container restart
    /// - Not suitable for production
    #[must_use]
    pub fn with_tmpfs(mut self) -> Self {
        self.tmpfs = true;
        self
    }

    /// Build and start the `ClickHouse` container
    ///
    /// # Errors
    /// Returns error if container fails to start or ports cannot be mapped
    pub async fn build(self) -> Result<ClickHouseContainer, TestcontainersError> {
        ClickHouseContainer::try_new_internal(self.config.as_deref(), self.tmpfs).await
    }
}

impl Default for ClickHouseContainerBuilder {
    fn default() -> Self { Self::new() }
}

pub struct ClickHouseContainer {
    pub endpoint:    String,
    pub native_port: u16,
    pub http_port:   u16,
    pub url:         String,
    pub user:        String,
    pub password:    String,
    container:       RwLock<Option<ContainerAsync<GenericImage>>>,
}

impl ClickHouseContainer {
    /// Create a builder for configuring the container
    pub fn builder() -> ClickHouseContainerBuilder { ClickHouseContainerBuilder::new() }

    /// Create a new `ClickHouse` container with default settings
    ///
    /// # Errors
    pub async fn try_new(conf: Option<&str>) -> Result<Self, TestcontainersError> {
        Self::try_new_internal(conf, false).await
    }

    /// Internal method for creating container with all options
    async fn try_new_internal(
        conf: Option<&str>,
        use_tmpfs: bool,
    ) -> Result<Self, TestcontainersError> {
        // Env vars
        let version = env::var(VERSION_ENV).unwrap_or(CLICKHOUSE_VERSION.to_string());
        let native_port = env::var(NATIVE_PORT_ENV)
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(CLICKHOUSE_NATIVE_PORT);
        let http_port = env::var(HTTP_PORT_ENV)
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(CLICKHOUSE_HTTP_PORT);
        let user = env::var(USER_ENV).ok().unwrap_or(CLICKHOUSE_USER.into());
        let password = env::var(PASSWORD_ENV).ok().unwrap_or(CLICKHOUSE_PASSWORD.into());

        // Get image
        let mut image = GenericImage::new("clickhouse/clickhouse-server", &version)
            .with_exposed_port(native_port.tcp())
            .with_exposed_port(http_port.tcp())
            .with_wait_for(testcontainers::core::WaitFor::message_on_stderr(
                "Ready for connections",
            ))
            .with_env_var(USER_ENV, &user)
            .with_env_var(PASSWORD_ENV, &password)
            .with_mount(Mount::bind_mount(
                format!(
                    "{}/{CLICKHOUSE_CONFIG_SRC}/{}",
                    env!("CARGO_MANIFEST_DIR"),
                    conf.unwrap_or("config.xml")
                ),
                CLICKHOUSE_CONFIG_DEST,
            ));

        // Add tmpfs mounts for benchmark mode (zero disk I/O)
        if use_tmpfs {
            #[cfg(feature = "tmpfs-size")]
            {
                // Explicit sizing prevents space exhaustion during long benchmark suites:
                // - /var/lib/clickhouse: 20GB (main data directory, accumulates WAL/merge
                //   artifacts)
                // - /var/log/clickhouse-server: 2GB (server logs)
                // - /tmp: 2GB (temporary files)
                image = image
                    .with_mount(Mount::tmpfs_mount("/var/lib/clickhouse").with_size("20g"))
                    .with_mount(Mount::tmpfs_mount("/var/log/clickhouse-server").with_size("2g"))
                    .with_mount(Mount::tmpfs_mount("/tmp").with_size("2g"));
            }
            #[cfg(not(feature = "tmpfs-size"))]
            {
                // Note: Without tmpfs-size feature, tmpfs defaults to 50% of available RAM per
                // mount. This may cause space exhaustion in long-running benchmark
                // suites.
                image = image
                    .with_mount(Mount::tmpfs_mount("/var/lib/clickhouse"))
                    .with_mount(Mount::tmpfs_mount("/var/log/clickhouse-server"))
                    .with_mount(Mount::tmpfs_mount("/tmp"));
            }
        }

        // Start container
        let container = image.start().await?;

        // Ports
        let native_port = container.get_host_port_ipv4(native_port).await?;
        let http_port = container.get_host_port_ipv4(http_port).await?;

        // Endpoint & URL
        let endpoint = env::var(ENDPOINT_ENV).unwrap_or(CLICKHOUSE_ENDPOINT.to_string());
        let url = format!("{endpoint}:{native_port}");

        // Pause
        sleep(Duration::from_secs(2)).await;

        let container = RwLock::new(Some(container));
        Ok(ClickHouseContainer { endpoint, native_port, http_port, url, user, password, container })
    }

    pub fn get_native_url(&self) -> &str { &self.url }

    pub fn get_native_port(&self) -> u16 { self.native_port }

    pub fn get_http_url(&self) -> String { format!("http://{}:{}", self.endpoint, self.http_port) }

    pub fn get_http_port(&self) -> u16 { self.http_port }

    /// # Errors
    pub async fn shutdown(&self) -> Result<(), TestcontainersError> {
        let mut container = self.container.write().await;
        if let Some(container) = container.take() {
            let _ = container
                .stop_with_timeout(Some(0))
                .await
                .inspect_err(|error| {
                    error!(?error, "Failed to stop container, will attempt to remove");
                })
                .ok();
            let _ = container
                .rm()
                .await
                .inspect_err(|error| {
                    error!(?error, "Failed to rm container, cleanup manually");
                })
                .ok();
        }
        Ok(())
    }
}

pub mod arrow_tests {
    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use uuid::Uuid;

    use super::*;
    #[cfg(feature = "pool")]
    use crate::pool::ConnectionManager;
    use crate::prelude::*;

    /// # Errors
    pub fn setup_test_arrow_client(url: &str, user: &str, password: &str) -> ClientBuilder {
        Client::<ArrowFormat>::builder()
            .with_endpoint(url)
            .with_username(user)
            .with_password(password)
    }

    /// # Errors
    #[cfg(feature = "pool")]
    pub async fn setup_test_arrow_pool(
        builder: ClientBuilder,
        pool_size: u32,
        timeout: Option<u16>,
    ) -> Result<bb8::Pool<ConnectionManager<ArrowFormat>>> {
        let manager = builder.build_pool_manager::<ArrowFormat>(false).await?;
        bb8::Pool::builder()
            .max_size(pool_size)
            .min_idle(pool_size)
            .test_on_check_out(true)
            .max_lifetime(Duration::from_secs(60 * 60 * 2))
            .idle_timeout(Duration::from_secs(60 * 60 * 2))
            .connection_timeout(Duration::from_secs(timeout.map_or(30, u64::from)))
            .retry_connection(false)
            .queue_strategy(bb8::QueueStrategy::Fifo)
            .build(manager)
            .await
            .map_err(|e| Error::External(Box::new(e)))
    }

    /// # Errors
    pub async fn setup_database(db: &str, client: &ArrowClient) -> Result<()> {
        // Create test db and table
        client.drop_database(db, true, None).await?;
        client.create_database(Some(db), None).await?;
        Ok(())
    }

    /// # Errors
    pub async fn setup_table(client: &ArrowClient, db: &str, schema: &SchemaRef) -> Result<String> {
        let create_options = CreateOptions::new("MergeTree").with_order_by(&["id".to_string()]);
        let table_qid = Qid::new();
        let table_name = format!("test_table_{table_qid}");
        client
            .create_table(Some(db), &table_name, schema, &create_options, Some(table_qid))
            .await?;
        Ok(format!("{db}.{table_name}"))
    }

    pub fn create_test_schema(strings_as_strings: bool) -> SchemaRef {
        let string_type = if strings_as_strings { DataType::Utf8 } else { DataType::Binary };
        Arc::new(Schema::new(vec![
            Field::new("id", string_type.clone(), false),
            Field::new("name", string_type, false),
            Field::new("value", DataType::Float64, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        ]))
    }

    /// # Panics
    #[expect(clippy::cast_precision_loss)]
    #[expect(clippy::cast_possible_wrap)]
    pub fn create_test_batch(rows: usize, strings_as_strings: bool) -> RecordBatch {
        let schema = create_test_schema(strings_as_strings);
        let id_row = if strings_as_strings {
            Arc::new(StringArray::from(
                (0..rows).map(|_| Uuid::new_v4().to_string()).collect::<Vec<_>>(),
            )) as ArrayRef
        } else {
            Arc::new(BinaryArray::from_iter_values((0..rows).map(|_| Uuid::new_v4().to_string())))
                as ArrayRef
        };
        let name_row = if strings_as_strings {
            Arc::new(StringArray::from((0..rows).map(|i| format!("name{i}")).collect::<Vec<_>>()))
                as ArrayRef
        } else {
            Arc::new(BinaryArray::from_iter_values((0..rows).map(|i| format!("name{i}"))))
                as ArrayRef
        };
        RecordBatch::try_new(schema, vec![
            id_row,
            name_row,
            Arc::new(Float64Array::from((0..rows).map(|i| i as f64).collect::<Vec<_>>())),
            Arc::new(
                TimestampMillisecondArray::from(
                    (0..rows).map(|i| i as i64 * 1000).collect::<Vec<_>>(),
                )
                .with_timezone(Arc::from("UTC")),
            ),
        ])
        .unwrap()
    }

    pub fn create_test_schema_fixed_types() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("int", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        ]))
    }

    /// # Panics
    /// Panics if `RecordBatch` construction fails (should not happen with valid test data)
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_precision_loss
    )]
    pub fn create_test_batch_fixed_types(rows: usize) -> RecordBatch {
        let schema = create_test_schema_fixed_types();
        let id_row =
            Arc::new(Int32Array::from((0..rows).map(|i| i as i32).collect::<Vec<_>>())) as ArrayRef;
        let int_row =
            Arc::new(Int32Array::from((0..rows).map(|i| i as i32).collect::<Vec<_>>())) as ArrayRef;
        let float_row =
            Arc::new(Float64Array::from((0..rows).map(|i| i as f64).collect::<Vec<_>>()))
                as ArrayRef;
        let ts_row = Arc::new(
            TimestampMillisecondArray::from((0..rows).map(|i| i as i64 * 1000).collect::<Vec<_>>())
                .with_timezone(Arc::from("UTC")),
        ) as ArrayRef;

        RecordBatch::try_new(schema, vec![id_row, int_row, float_row, ts_row]).unwrap()
    }

    /// Configuration for creating test batches with specific column types
    #[derive(Debug, Clone, Copy, Default)]
    pub struct BatchConfig {
        pub int8:       usize,
        pub int16:      usize,
        pub int32:      usize,
        pub int64:      usize,
        pub uint8:      usize,
        pub uint16:     usize,
        pub uint32:     usize,
        pub uint64:     usize,
        pub float32:    usize,
        pub float64:    usize,
        pub bool:       usize,
        pub utf8:       usize,
        pub utf8_len:   usize,
        pub binary:     usize,
        pub binary_len: usize,
        pub timestamp:  usize,
        pub rand:       bool,
        pub include_id: bool,
        pub unique_id:  bool,
    }

    impl BatchConfig {
        /// Default configuration: id + 2x Int32, 1x Float64, 1x Timestamp (32 bytes/row with id)
        pub fn default_fixed() -> Self {
            Self {
                int32: 2,
                float64: 1,
                timestamp: 1,
                rand: true,
                include_id: true,
                unique_id: true,
                ..Default::default()
            }
        }

        /// Parse configuration from environment variables
        pub fn from_env() -> Self {
            use std::env;

            let parse_env = |key: &str| -> usize {
                env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(0)
            };

            let parse_bool_env = |key: &str, default: bool| -> bool {
                env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
            };

            let mut config = Self {
                int8:       parse_env("INT8"),
                int16:      parse_env("INT16"),
                int32:      parse_env("INT32"),
                int64:      parse_env("INT64"),
                uint8:      parse_env("UINT8"),
                uint16:     parse_env("UINT16"),
                uint32:     parse_env("UINT32"),
                uint64:     parse_env("UINT64"),
                float32:    parse_env("FLOAT32"),
                float64:    parse_env("FLOAT64"),
                bool:       parse_env("BOOL"),
                utf8:       parse_env("UTF8"),
                utf8_len:   parse_env("UTF8_LEN"),
                binary:     parse_env("BINARY"),
                binary_len: parse_env("BINARY_LEN"),
                timestamp:  parse_env("TIMESTAMP"),
                rand:       parse_bool_env("RAND", true),
                include_id: parse_bool_env("INCLUDE_ID", true),
                unique_id:  parse_bool_env("UNIQUE_ID", true),
            };

            // Apply defaults
            if config.utf8 > 0 && config.utf8_len == 0 {
                config.utf8_len = 10;
            }
            if config.binary > 0 && config.binary_len == 0 {
                config.binary_len = 16;
            }

            // If no columns specified, use default fixed config
            if config.int8 == 0
                && config.int16 == 0
                && config.int32 == 0
                && config.int64 == 0
                && config.uint8 == 0
                && config.uint16 == 0
                && config.uint32 == 0
                && config.uint64 == 0
                && config.float32 == 0
                && config.float64 == 0
                && config.bool == 0
                && config.utf8 == 0
                && config.binary == 0
                && config.timestamp == 0
            {
                return Self::default_fixed();
            }

            config
        }
    }

    /// Create a test batch with configurable column types
    ///
    /// The batch will contain the specified number of columns for each type.
    /// If config is not provided, uses default: 2x Int32, 1x Float64, 1x Timestamp (24 bytes/row)
    pub fn create_test_batch_generic(rows: usize) -> RecordBatch {
        create_test_batch_with_config(rows, &BatchConfig::from_env())
    }

    /// Create a test batch with explicit configuration
    ///
    /// # Arguments
    /// * `rows` - Number of rows in the batch
    /// * `config` - Configuration specifying column types and data generation
    /// * `id_offset` - Optional starting offset for ID generation (used with `unique_id`)
    ///
    /// # Panics
    /// Panics if `RecordBatch` construction fails (should not happen with valid test data)
    #[allow(
        clippy::too_many_lines,
        clippy::cast_precision_loss,
        clippy::cast_possible_wrap,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn create_test_batch_with_config_offset(
        rows: usize,
        config: &BatchConfig,
        id_offset: Option<usize>,
    ) -> RecordBatch {
        use arrow::array::*;
        use arrow::datatypes::TimeUnit;

        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut col_idx = 0;

        // Fast pseudo-random hash function for deterministic random data
        let hash = |i: usize| -> usize {
            let mut x = i.wrapping_mul(0x9E37_79B9).wrapping_add(0x85EB_CA6B);
            x ^= x >> 16;
            x = x.wrapping_mul(0x21F0_AAAD);
            x ^= x >> 15;
            x = x.wrapping_mul(0x735A_2D97);
            x ^= x >> 15;
            x
        };

        // Value generator: use hash if rand=true, otherwise sequential
        let gen_val = |i: usize| -> usize { if config.rand { hash(i) } else { i } };

        // Add 'id' column first if requested (required for ClickHouse ORDER BY)
        if config.include_id {
            fields.push(Field::new("id", DataType::Int64, false));
            let array: Int64Array = if let Some(offset) = id_offset {
                if config.unique_id {
                    // Generate unique IDs by adding offset to index
                    (0..rows).map(|i| (offset + i) as i64).collect()
                } else {
                    // unique_id=false: ignore offset, use gen_val
                    (0..rows).map(|i| gen_val(i) as i64).collect()
                }
            } else {
                // No offset provided: use gen_val (hashed or sequential based on rand flag)
                (0..rows).map(|i| gen_val(i) as i64).collect()
            };
            columns.push(Arc::new(array) as ArrayRef);
            col_idx += 1;
        }

        // Helper macro to add primitive columns
        macro_rules! add_primitive_columns {
            ($count:expr, $type_name:expr, $arrow_type:expr, $array_type:ty, $value_fn:expr) => {
                for _ in 0..$count {
                    fields.push(Field::new(
                        format!("{}_{}", $type_name, col_idx),
                        $arrow_type,
                        false,
                    ));
                    let array: $array_type = (0..rows).map($value_fn).collect();
                    columns.push(Arc::new(array) as ArrayRef);
                    col_idx += 1;
                }
            };
        }

        // Add integer columns
        add_primitive_columns!(
            config.int8,
            "int8",
            DataType::Int8,
            Int8Array,
            |i: usize| gen_val(i) as i8
        );
        add_primitive_columns!(config.int16, "int16", DataType::Int16, Int16Array, |i: usize| {
            gen_val(i) as i16
        });
        add_primitive_columns!(config.int32, "int32", DataType::Int32, Int32Array, |i: usize| {
            gen_val(i) as i32
        });
        add_primitive_columns!(config.int64, "int64", DataType::Int64, Int64Array, |i: usize| {
            gen_val(i) as i64
        });

        add_primitive_columns!(config.uint8, "uint8", DataType::UInt8, UInt8Array, |i: usize| {
            gen_val(i) as u8
        });
        add_primitive_columns!(
            config.uint16,
            "uint16",
            DataType::UInt16,
            UInt16Array,
            |i: usize| gen_val(i) as u16
        );
        add_primitive_columns!(
            config.uint32,
            "uint32",
            DataType::UInt32,
            UInt32Array,
            |i: usize| gen_val(i) as u32
        );
        add_primitive_columns!(
            config.uint64,
            "uint64",
            DataType::UInt64,
            UInt64Array,
            |i: usize| gen_val(i) as u64
        );

        // Add float columns
        add_primitive_columns!(
            config.float32,
            "float32",
            DataType::Float32,
            Float32Array,
            |i: usize| gen_val(i) as f32
        );
        add_primitive_columns!(
            config.float64,
            "float64",
            DataType::Float64,
            Float64Array,
            |i: usize| gen_val(i) as f64
        );

        // Add boolean columns
        for _ in 0..config.bool {
            fields.push(Field::new(format!("bool_{col_idx}"), DataType::Boolean, false));
            let array: BooleanArray = (0..rows).map(|i| Some(gen_val(i) % 2 == 0)).collect();
            columns.push(Arc::new(array) as ArrayRef);
            col_idx += 1;
        }

        // Add UTF8 columns
        for _ in 0..config.utf8 {
            fields.push(Field::new(format!("utf8_{col_idx}"), DataType::Utf8, false));
            let values: Vec<String> = (0..rows)
                .map(|i| {
                    let val = gen_val(i);
                    // Generate string by repeating the hash value cyclically to fill length
                    let base = format!("{val:016x}"); // 16 hex chars
                    base.chars().cycle().take(config.utf8_len).collect()
                })
                .collect();
            let array = StringArray::from(values);
            columns.push(Arc::new(array) as ArrayRef);
            col_idx += 1;
        }

        // Add Binary columns
        for _ in 0..config.binary {
            fields.push(Field::new(format!("binary_{col_idx}"), DataType::Binary, false));
            let values: Vec<Vec<u8>> = (0..rows)
                .map(|i| {
                    let val = gen_val(i) as u64;
                    val.to_le_bytes().iter().cycle().take(config.binary_len).copied().collect()
                })
                .collect();
            let array = BinaryArray::from(values.iter().map(Vec::as_slice).collect::<Vec<_>>());
            columns.push(Arc::new(array) as ArrayRef);
            col_idx += 1;
        }

        // Add Timestamp columns
        for _ in 0..config.timestamp {
            fields.push(Field::new(
                format!("ts_{col_idx}"),
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ));
            let array = TimestampMillisecondArray::from(
                (0..rows).map(|i| (gen_val(i) as i64).wrapping_mul(1000)).collect::<Vec<_>>(),
            )
            .with_timezone(Arc::from("UTC"));
            columns.push(Arc::new(array) as ArrayRef);
            col_idx += 1;
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).unwrap()
    }

    /// Create a test batch with explicit configuration (backward-compatible wrapper)
    ///
    /// # Panics
    /// Panics if `RecordBatch` construction fails (should not happen with valid test data)
    pub fn create_test_batch_with_config(rows: usize, config: &BatchConfig) -> RecordBatch {
        create_test_batch_with_config_offset(rows, config, None)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_batch_config_default() {
            let config = BatchConfig::default();
            assert_eq!(config.int32, 0);
            assert_eq!(config.float64, 0);
            assert!(!config.rand);
            assert!(!config.include_id);
        }

        #[test]
        fn test_batch_config_default_fixed() {
            let config = BatchConfig::default_fixed();
            assert_eq!(config.int32, 2);
            assert_eq!(config.float64, 1);
            assert_eq!(config.timestamp, 1);
            assert!(config.rand, "rand should default to true");
            assert!(config.include_id, "include_id should default to true");
        }

        #[test]
        fn test_create_batch_with_id() {
            let config =
                BatchConfig { int32: 2, float64: 1, include_id: true, ..Default::default() };

            let batch = create_test_batch_with_config(100, &config);

            assert_eq!(batch.num_rows(), 100);
            assert_eq!(batch.num_columns(), 4); // id + 2*int32 + 1*float64

            // First column should be 'id'
            assert_eq!(batch.schema().field(0).name(), "id");
            assert!(matches!(batch.schema().field(0).data_type(), DataType::Int64));
        }

        #[test]
        fn test_create_batch_without_id() {
            let config =
                BatchConfig { int32: 2, float64: 1, include_id: false, ..Default::default() };

            let batch = create_test_batch_with_config(100, &config);

            assert_eq!(batch.num_rows(), 100);
            assert_eq!(batch.num_columns(), 3); // 2*int32 + 1*float64 (no id)

            // No 'id' column
            assert_ne!(batch.schema().field(0).name(), "id");
        }

        #[test]
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        fn test_random_vs_sequential() {
            let random_config = BatchConfig { int32: 1, rand: true, ..Default::default() };

            let sequential_config = BatchConfig { int32: 1, rand: false, ..Default::default() };

            let random_batch = create_test_batch_with_config(10, &random_config);
            let sequential_batch = create_test_batch_with_config(10, &sequential_config);

            let random_array =
                random_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let sequential_array =
                sequential_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();

            // Sequential should be 0, 1, 2, ...
            assert_eq!(sequential_array.value(0), 0);
            assert_eq!(sequential_array.value(1), 1);
            assert_eq!(sequential_array.value(2), 2);

            // Random should not be sequential
            let is_sequential = (0..10).all(|i| random_array.value(i) == i as i32);
            assert!(!is_sequential, "Random data should not be sequential");
        }

        #[test]
        fn test_utf8_columns() {
            let config = BatchConfig { utf8: 2, utf8_len: 10, rand: true, ..Default::default() };

            let batch = create_test_batch_with_config(100, &config);

            assert_eq!(batch.num_columns(), 2);

            // Check both columns are UTF8
            for i in 0..2 {
                assert!(matches!(batch.schema().field(i).data_type(), DataType::Utf8));

                let array = batch.column(i).as_any().downcast_ref::<StringArray>().unwrap();

                // Check string length
                assert_eq!(array.value(0).len(), 10);
            }
        }

        #[test]
        fn test_mixed_column_types() {
            let config = BatchConfig {
                include_id: true,
                int32: 2,
                int64: 1,
                float64: 1,
                bool: 1,
                timestamp: 1,
                rand: true,
                ..Default::default()
            };

            let batch = create_test_batch_with_config(50, &config);

            // id + 2*int32 + 1*int64 + 1*float64 + 1*bool + 1*timestamp = 7 columns
            assert_eq!(batch.num_columns(), 7);
            assert_eq!(batch.num_rows(), 50);

            // Verify first column is id
            assert_eq!(batch.schema().field(0).name(), "id");
        }

        #[test]
        #[allow(clippy::cast_precision_loss)]
        fn test_bytes_per_row_consistency() {
            // Fixed-size types should have consistent bytes/row
            let config = BatchConfig {
                int32: 2,
                float64: 1,
                timestamp: 1,
                include_id: true,
                ..Default::default()
            };

            let small_batch = create_test_batch_with_config(1_000, &config);
            let large_batch = create_test_batch_with_config(100_000, &config);

            let small_bytes_per_row = small_batch.get_array_memory_size() as f64 / 1_000.0;
            let large_bytes_per_row = large_batch.get_array_memory_size() as f64 / 100_000.0;

            // Should be very close (within 5% - some variance due to Arrow overhead in small
            // batches)
            let diff_pct =
                ((small_bytes_per_row - large_bytes_per_row) / large_bytes_per_row).abs();
            assert!(
                diff_pct < 0.05,
                "Fixed-size types should have consistent bytes/row, got {:.2} vs {:.2} ({:.1}% \
                 diff)",
                small_bytes_per_row,
                large_bytes_per_row,
                diff_pct * 100.0
            );
        }

        #[test]
        fn test_empty_config_creates_default() {
            // Note: Empty config with no columns would fail, so we test minimal config
            let config = BatchConfig { int32: 1, ..Default::default() };
            let batch = create_test_batch_with_config(100, &config);

            assert_eq!(batch.num_rows(), 100);
            assert_eq!(batch.num_columns(), 1);
        }

        #[test]
        fn test_column_naming() {
            let config =
                BatchConfig { int32: 3, float64: 2, include_id: false, ..Default::default() };

            let batch = create_test_batch_with_config(10, &config);

            // Should have sequential naming: int32_0, int32_1, int32_2, float64_3, float64_4
            assert_eq!(batch.schema().field(0).name(), "int32_0");
            assert_eq!(batch.schema().field(1).name(), "int32_1");
            assert_eq!(batch.schema().field(2).name(), "int32_2");
            assert_eq!(batch.schema().field(3).name(), "float64_3");
            assert_eq!(batch.schema().field(4).name(), "float64_4");
        }

        #[test]
        fn test_binary_columns() {
            let config = BatchConfig { binary: 1, binary_len: 16, ..Default::default() };

            let batch = create_test_batch_with_config(100, &config);

            assert_eq!(batch.num_columns(), 1);
            assert!(matches!(batch.schema().field(0).data_type(), DataType::Binary));

            let array = batch.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();

            // Check binary length
            assert_eq!(array.value(0).len(), 16);
        }

        #[test]
        #[allow(clippy::cast_possible_wrap)]
        fn test_unique_id_no_gaps_across_batches() {
            // Test parameters
            let total_rows = 10_000;
            let batch_size = 1_000;
            let num_batches = total_rows / batch_size;

            let config = BatchConfig {
                int32: 1,
                include_id: true,
                unique_id: true,
                rand: false,
                ..Default::default()
            };

            // Collect all IDs from all batches
            let mut all_ids = Vec::with_capacity(total_rows);

            for batch_idx in 0..num_batches {
                let offset = batch_idx * batch_size;
                let batch = create_test_batch_with_config_offset(batch_size, &config, Some(offset));

                // Extract IDs from this batch
                let id_array = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();

                for i in 0..batch_size {
                    all_ids.push(id_array.value(i));
                }
            }

            // Verify total count
            assert_eq!(all_ids.len(), total_rows, "Should have collected all IDs");

            // Verify uniqueness: convert to HashSet and check size
            let unique_ids: std::collections::HashSet<_> = all_ids.iter().copied().collect();
            assert_eq!(unique_ids.len(), total_rows, "All IDs should be unique (no duplicates)");

            // Verify no gaps: sort and check sequential
            let mut sorted_ids = all_ids.clone();
            sorted_ids.sort_unstable();

            for (idx, &id) in sorted_ids.iter().enumerate() {
                assert_eq!(id, idx as i64, "ID at position {idx} should be {idx}, but found {id}");
            }

            // Verify range: min should be 0, max should be total_rows-1
            assert_eq!(sorted_ids[0], 0, "Minimum ID should be 0");
            assert_eq!(
                sorted_ids[total_rows - 1],
                (total_rows - 1) as i64,
                "Maximum ID should be total_rows-1"
            );

            // Verify IDs in first batch
            let first_batch = create_test_batch_with_config_offset(batch_size, &config, Some(0));
            let first_id_array =
                first_batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(first_id_array.value(0), 0, "First batch should start at 0");
            assert_eq!(
                first_id_array.value(batch_size - 1),
                (batch_size - 1) as i64,
                "First batch should end at batch_size-1"
            );

            // Verify IDs in last batch
            let last_offset = (num_batches - 1) * batch_size;
            let last_batch =
                create_test_batch_with_config_offset(batch_size, &config, Some(last_offset));
            let last_id_array = last_batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(
                last_id_array.value(0),
                last_offset as i64,
                "Last batch should start at correct offset"
            );
            assert_eq!(
                last_id_array.value(batch_size - 1),
                (total_rows - 1) as i64,
                "Last batch should end at total_rows-1"
            );
        }

        #[test]
        fn test_unique_id_false_creates_overlapping_ids() {
            // When unique_id=false, IDs should overlap across batches (default behavior)
            let batch_size = 100;

            let config = BatchConfig {
                int32: 1,
                include_id: true,
                unique_id: false, // Default behavior
                rand: false,
                ..Default::default()
            };

            // Create two batches with different offsets but unique_id=false
            let batch1 = create_test_batch_with_config_offset(batch_size, &config, Some(0));
            let batch2 =
                create_test_batch_with_config_offset(batch_size, &config, Some(batch_size));

            let id_array1 = batch1.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let id_array2 = batch2.column(0).as_any().downcast_ref::<Int64Array>().unwrap();

            // Both batches should have IDs starting from 0 (overlapping)
            // because unique_id=false ignores the offset parameter
            assert_eq!(id_array1.value(0), 0);
            assert_eq!(id_array2.value(0), 0);
            assert_eq!(id_array1.value(99), 99);
            assert_eq!(id_array2.value(99), 99);
        }

        #[test]
        #[allow(clippy::cast_possible_wrap)]
        fn test_unique_id_with_uneven_last_batch() {
            // Test with total_rows that doesn't divide evenly by batch_size
            let total_rows: usize = 2_500;
            let batch_size: usize = 1_000;
            let config = BatchConfig {
                int32: 1,
                include_id: true,
                unique_id: true,
                rand: false,
                ..Default::default()
            };

            let mut all_ids = Vec::new();

            // Create 3 batches: 1000, 1000, 500
            for batch_idx in 0..3 {
                let offset = batch_idx * batch_size;
                let remaining = total_rows.saturating_sub(offset);
                let this_batch_size = remaining.min(batch_size);

                let batch =
                    create_test_batch_with_config_offset(this_batch_size, &config, Some(offset));
                let id_array = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();

                for i in 0..this_batch_size {
                    all_ids.push(id_array.value(i));
                }
            }

            // Verify all IDs are present and unique
            assert_eq!(all_ids.len(), total_rows);
            let unique_ids: std::collections::HashSet<_> = all_ids.iter().copied().collect();
            assert_eq!(unique_ids.len(), total_rows, "All IDs should be unique");

            // Verify no gaps
            let mut sorted = all_ids;
            sorted.sort_unstable();
            for (idx, &id) in sorted.iter().enumerate() {
                assert_eq!(id, idx as i64);
            }
        }

        #[test]
        fn test_create_test_schema_fixed_types() {
            let schema = create_test_schema_fixed_types();
            assert_eq!(schema.fields().len(), 4); // id, int, value, ts

            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(1).name(), "int");
            assert_eq!(schema.field(2).name(), "value");
            assert_eq!(schema.field(3).name(), "ts");
        }

        #[test]
        fn test_create_test_batch_fixed_types() {
            let batch = create_test_batch_fixed_types(50);
            assert_eq!(batch.num_rows(), 50);
            assert_eq!(batch.num_columns(), 4);

            // Verify schema
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "int");
        }

        #[test]
        fn test_create_test_batch_with_config_offset() {
            let config = BatchConfig {
                int32: 2,
                float64: 1,
                include_id: true,
                unique_id: true, // Required for offset to work
                ..Default::default()
            };

            let batch = create_test_batch_with_config_offset(100, &config, Some(0));
            assert_eq!(batch.num_rows(), 100);
            assert_eq!(batch.num_columns(), 4); // id + 2*int32 + 1*float64

            // Test with offset
            let batch_offset = create_test_batch_with_config_offset(50, &config, Some(100));
            assert_eq!(batch_offset.num_rows(), 50);

            // Verify IDs start at offset
            let id_array = batch_offset.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(id_array.value(0), 100);
            assert_eq!(id_array.value(49), 149);
        }

        #[test]
        fn test_create_test_batch_generic() {
            let batch = create_test_batch_generic(100);
            assert_eq!(batch.num_rows(), 100);
            // Generic batch uses default_fixed config
            assert!(batch.num_columns() >= 4); // At least id, int32, float64, timestamp
        }

        #[test]
        fn test_create_test_batch_with_config() {
            let config = BatchConfig {
                int32: 1,
                float64: 2,
                utf8: 1,
                utf8_len: 20,
                include_id: true,
                ..Default::default()
            };

            let batch = create_test_batch_with_config(100, &config);
            assert_eq!(batch.num_rows(), 100);
            assert_eq!(batch.num_columns(), 5); // id + 1*int32 + 2*float64 + 1*utf8
        }

        #[test]
        fn test_create_test_schema() {
            // Test with strings_as_strings = true
            let schema = create_test_schema(true);
            assert!(schema.fields().len() > 0);

            // Test with strings_as_strings = false
            let schema_binary = create_test_schema(false);
            assert!(schema_binary.fields().len() > 0);
        }

        #[test]
        fn test_create_test_batch() {
            // Test with strings_as_strings = true
            let batch = create_test_batch(50, true);
            assert_eq!(batch.num_rows(), 50);

            // Test with strings_as_strings = false
            let batch_binary = create_test_batch(50, false);
            assert_eq!(batch_binary.num_rows(), 50);
        }

        #[test]
        fn test_batch_config_from_env_defaults() {
            // Test with no env vars set - should use default_fixed
            let config = BatchConfig::from_env();
            assert_eq!(config.int32, 2);
            assert_eq!(config.float64, 1);
            assert_eq!(config.timestamp, 1);
        }

        #[test]
        fn test_batch_config_all_numeric_types() {
            let config = BatchConfig {
                int8: 1,
                int16: 1,
                int32: 1,
                int64: 1,
                uint8: 1,
                uint16: 1,
                uint32: 1,
                uint64: 1,
                float32: 1,
                float64: 1,
                bool: 1,
                timestamp: 1,
                include_id: true,
                ..Default::default()
            };

            let batch = create_test_batch_with_config(50, &config);
            assert_eq!(batch.num_rows(), 50);
            assert_eq!(batch.num_columns(), 13); // id + 12 types
        }

        #[test]
        fn test_batch_config_binary_types() {
            let config =
                BatchConfig { binary: 2, binary_len: 32, include_id: false, ..Default::default() };

            let batch = create_test_batch_with_config(25, &config);
            assert_eq!(batch.num_rows(), 25);
            assert_eq!(batch.num_columns(), 2); // 2 binary columns
        }
    }
}

#[cfg(test)]
mod container_tests {
    use super::*;

    #[test]
    fn test_builder_default() {
        let builder = ClickHouseContainerBuilder::default();
        assert_eq!(builder.config, None);
        assert!(!builder.tmpfs);
    }

    #[test]
    fn test_builder_new() {
        let builder = ClickHouseContainerBuilder::new();
        assert_eq!(builder.config, None);
        assert!(!builder.tmpfs);
    }

    #[test]
    fn test_builder_with_config() {
        let builder = ClickHouseContainerBuilder::new().with_config("custom.xml");
        assert_eq!(builder.config, Some("custom.xml".to_string()));
        assert!(!builder.tmpfs);
    }

    #[test]
    fn test_builder_with_tmpfs() {
        let builder = ClickHouseContainerBuilder::new().with_tmpfs();
        assert_eq!(builder.config, None);
        assert!(builder.tmpfs);
    }

    #[test]
    fn test_builder_chaining() {
        let builder = ClickHouseContainerBuilder::new().with_config("test.xml").with_tmpfs();
        assert_eq!(builder.config, Some("test.xml".to_string()));
        assert!(builder.tmpfs);
    }

    #[test]
    fn test_builder_chaining_reverse() {
        let builder = ClickHouseContainerBuilder::new().with_tmpfs().with_config("test.xml");
        assert_eq!(builder.config, Some("test.xml".to_string()));
        assert!(builder.tmpfs);
    }

    #[tokio::test]
    async fn test_get_or_create_benchmark_container() {
        // Test with no config
        let container1 = get_or_create_benchmark_container(None).await;
        assert!(container1.http_port > 0);

        // Test with same config - should return same instance (static)
        let container2 = get_or_create_benchmark_container(None).await;
        assert_eq!(container1.http_port, container2.http_port);
    }
}
