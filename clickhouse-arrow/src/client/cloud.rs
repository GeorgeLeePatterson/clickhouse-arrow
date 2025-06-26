use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use tokio::sync::oneshot;
use tracing::*;

use crate::spawn::SpawnedTask;

/// Track whether cloud service has been started
pub(crate) static CLOUD_START: LazyLock<Arc<AtomicBool>> =
    LazyLock::new(|| Arc::new(AtomicBool::new(false)));

const CLOUD_WAKEUP_TIMEOUT: u64 = 300;
const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                                  (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";
const DEFAULT_SEC_CH_UA: &str =
    "\"Not A(Brand\";v=\"24\", \"Chromium\";v=\"121\", \"Google Chrome\";v=\"121\"";

pub(super) async fn ping_cloud(
    endpoint: String,
    timeout: Option<u64>,
    track: Option<&AtomicBool>,
    cancel: oneshot::Receiver<()>,
) {
    // In the case of ClickHouseCloud the instance can go to sleep. This will attempt to wake.
    let Some(e) = endpoint.split(':').next().filter(|s| s.ends_with("cloud")) else {
        warn!(endpoint, "Cloud endpoint malformed");
        return;
    };

    if track.unwrap_or(&CLOUD_START).load(Ordering::Relaxed) {
        debug!("Cloud service already started");
        return;
    }

    let ping = cloud_service_wakeup(format!("https://{e}:8443/ping"), timeout);

    tokio::select! {
        res = ping => match res {
            Ok(message) => {
                tracing::info!(?message, "ClickHouse cloud service started");

                // Update cloud start tracker
                track.unwrap_or(&CLOUD_START).store(true, Ordering::Relaxed);
            }
            Err(error) => tracing::error!(?error, "ClickHouse error starting cloud service"),
        },
        _ = cancel => {
            tracing::info!("Cloud service cancelled");
        }
    }
}

async fn cloud_service_wakeup(
    endpoint: String,
    timeout: Option<u64>,
) -> Result<String, ureq::Error> {
    let wakeup_timeout = std::env::var("CLICKHOUSE_CLOUD_WAKEUP_TIMEOUT")
        .ok()
        .and_then(|t| t.parse::<u64>().ok())
        .or(timeout)
        .unwrap_or(CLOUD_WAKEUP_TIMEOUT);
    let wakeup_timeout =
        if wakeup_timeout == 0 { None } else { Some(Duration::from_secs(wakeup_timeout)) };
    SpawnedTask::spawn_blocking(move || {
        tracing::trace!("pinging cloud instance @ {endpoint}");
        ureq::get(&endpoint)
            .header("Accept", "*/*")
            .header("sec-ch-ua", DEFAULT_SEC_CH_UA)
            .config()
            .timeout_global(wakeup_timeout)
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .call()?
            .body_mut()
            .read_to_string()
    })
    .join_unwind()
    .await
    .map_err(|error| ureq::Error::Other(Box::new(error)))?
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn test_ping_cloud() {
        let (_, c) = oneshot::channel();
        ping_cloud(String::new(), None, None, c).await;

        let track = Arc::new(AtomicBool::new(true));
        let (_, c) = oneshot::channel();
        ping_cloud(String::new(), None, Some(&track), c).await;
    }
}
