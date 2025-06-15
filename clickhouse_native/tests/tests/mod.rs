pub mod arrow;
pub mod compat;
pub mod native;

use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, LazyLock};

use clickhouse_native::test_utils::{self, ClickHouseContainer};
use futures_util::FutureExt;
use tracing::{debug, error};

use crate::common::constants::{DISABLE_CLEANUP_ENV, DISABLE_CLEANUP_ON_ERROR_ENV};

/// Track how many tests have started and finished, for cleanup
pub static TESTS_RUNNING: LazyLock<Arc<AtomicU8>> = LazyLock::new(|| Arc::new(AtomicU8::new(0)));

/// Macro to run tests using the below test harness.
#[macro_export]
macro_rules! e2e_test {
    ($name:ident, $test_fn:expr, $dirs:expr, $conf:expr) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            let name = stringify!($name);
            let result =
                $crate::tests::run_test_with_cleanup(name, $test_fn, Some($dirs), $conf).await;
            if let Err(panic) = result {
                std::panic::resume_unwind(panic);
            }
        }
    };
}

/// Test harness for catching panics and attempting to shutdown the container
///
/// # Errors
/// # Panics
pub async fn run_test_with_cleanup<F, Fut>(
    name: &str,
    test_fn: F,
    directives: Option<&[(&str, &str)]>,
    clickhouse_conf: Option<&str>,
) -> Result<(), Box<dyn std::any::Any + Send>>
where
    F: FnOnce(Arc<ClickHouseContainer>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let disable_cleanup = std::env::var(DISABLE_CLEANUP_ENV)
        .ok()
        .is_some_and(|e| e.eq_ignore_ascii_case("true") || e == "1");

    let disable_cleanup_on_error = std::env::var(DISABLE_CLEANUP_ON_ERROR_ENV)
        .ok()
        .is_some_and(|e| e.eq_ignore_ascii_case("true") || e == "1");

    // Initialize container and tracing
    test_utils::init_tracing(directives);
    let ch = test_utils::create_container(clickhouse_conf).await;

    let result = AssertUnwindSafe(test_fn(Arc::clone(&ch))).catch_unwind().await;

    // Either path will not update TESTS_RUNNING, and will keep containers running
    if disable_cleanup || (disable_cleanup_on_error && result.is_err()) {
        if result.is_err() {
            error!(">>> Exiting test w/o shutdown: {name}");
        } else {
            debug!(">>> Exiting test w/o shutdown: {name}");
        }
        return result;
    }

    ch.shutdown().await.expect("Shutting down container");

    result
}
