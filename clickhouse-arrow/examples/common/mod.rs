use std::panic::AssertUnwindSafe;

use clickhouse_arrow::test_utils::{ClickHouseContainer, get_or_create_container, init_tracing};
use futures_util::FutureExt;

#[allow(unused)]
pub(crate) const DB_NAME: &str = "example_insert_test";
#[allow(unused)]
pub(crate) const ROWS: usize = 100_000;
const DISABLE_CLEANUP_ENV: &str = "DISABLE_CLEANUP";

pub(crate) fn init(directives: Option<&[(&str, &str)]>) {
    if let Ok(l) = std::env::var("RUST_LOG")
        && !l.is_empty()
    {
        // Add directives here
        init_tracing(directives);
    }
}

pub(crate) async fn setup(directives: Option<&[(&str, &str)]>) -> &'static ClickHouseContainer {
    // Init tracing
    init(directives);
    // Setup container
    get_or_create_container(None).await
}

/// Test harness for catching panics and attempting to shutdown the container
///
/// # Errors
/// # Panics
pub(crate) async fn run_example_with_cleanup<F, Fut>(
    example: F,
    directives: Option<&[(&str, &str)]>,
) -> Result<(), Box<dyn std::any::Any + Send>>
where
    F: FnOnce(&'static ClickHouseContainer) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    // Initialize container and tracing
    let ch = setup(directives).await;
    let result = AssertUnwindSafe(example(ch)).catch_unwind().await;
    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return result;
    }
    ch.shutdown().await.expect("Shutting down container");
    result
}
