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

pub struct ClickHouseContainer {
    pub endpoint: String,
    pub native_port: u16,
    pub http_port: u16,
    pub url: String,
    pub user: String,
    pub password: String,
    container: RwLock<Option<ContainerAsync<GenericImage>>>,
}

impl ClickHouseContainer {
    /// # Errors
    pub async fn try_new(conf: Option<&str>) -> Result<Self, TestcontainersError> {
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
        let image = GenericImage::new("clickhouse/clickhouse-server", &version)
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

    pub fn get_native_url(&self) -> &str {
        &self.url
    }

    pub fn get_native_port(&self) -> u16 {
        self.native_port
    }

    pub fn get_http_url(&self) -> String {
        format!("http://{}:{}", self.endpoint, self.http_port)
    }

    pub fn get_http_port(&self) -> u16 {
        self.http_port
    }

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
        RecordBatch::try_new(
            schema,
            vec![
                id_row,
                name_row,
                Arc::new(Float64Array::from((0..rows).map(|i| i as f64).collect::<Vec<_>>())),
                Arc::new(
                    TimestampMillisecondArray::from(
                        (0..rows).map(|i| i as i64 * 1000).collect::<Vec<_>>(),
                    )
                    .with_timezone(Arc::from("UTC")),
                ),
            ],
        )
        .unwrap()
    }
}
