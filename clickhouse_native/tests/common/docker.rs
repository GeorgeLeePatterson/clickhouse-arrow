use std::env;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use testcontainers::core::{IntoContainerPort, Mount};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, TestcontainersError};
use tokio::sync::RwLock;
use tokio::time::{Instant, sleep};
use tracing::{debug, error};

use super::constants::*;
use crate::tests::TESTS_RUNNING;

const CLICKHOUSE_CONFIG_SRC: &str = "tests/bin/config.xml";
const CLICKHOUSE_CONFIG_DEST: &str = "/etc/clickhouse-server/config.xml";

// Env defaults
const CLICKHOUSE_USER: &str = "clickhouse";
const CLICKHOUSE_PASSWORD: &str = "clickhouse";
const CLICKHOUSE_VERSION: &str = "latest";
const CLICKHOUSE_NATIVE_PORT: u16 = 9000;
const CLICKHOUSE_HTTP_PORT: u16 = 8123;
const CLICKHOUSE_ENDPOINT: &str = "localhost";

pub static CONTAINER: OnceLock<Arc<ClickHouseContainer>> = OnceLock::new();

/// # Panics
/// You bet it panics. You better be careful.
pub async fn get_clickhouse_container(name: &str) -> &'static Arc<ClickHouseContainer> {
    if let Some(c) = CONTAINER.get() {
        c
    } else {
        if TESTS_RUNNING.fetch_add(1, Ordering::SeqCst) == 0 {
            debug!(">>> ({name}) Setting start flag for container");
            let ch = ClickHouseContainer::try_new()
                .await
                .expect("Failed to initialize ClickHouse container");
            return CONTAINER.get_or_init(|| Arc::new(ch));
        }

        let max = Duration::from_secs(5);
        let start = Instant::now();
        loop {
            if let Some(c) = CONTAINER.get() {
                debug!(">>> ({name}) Container set, returning");
                return c;
            }

            assert!(
                max.checked_sub(start.elapsed()).is_some(),
                "Could not get or create ClickHouseContainer"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }
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
    /// # Errors
    pub async fn try_new() -> Result<Self, TestcontainersError> {
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
                format!("{}/{CLICKHOUSE_CONFIG_SRC}", env!("CARGO_MANIFEST_DIR")),
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

    pub fn get_native_url(&self) -> &str { &self.url }

    pub fn get_http_url(&self) -> String { format!("http://{}:{}", self.endpoint, self.http_port) }

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
