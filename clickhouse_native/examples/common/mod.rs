#![allow(unused)]
use std::panic::AssertUnwindSafe;

use clickhouse_native::test_utils::{ClickHouseContainer, get_or_create_container, init_tracing};
use futures_util::FutureExt;
use tokio::time::Instant;

pub(crate) const DB_NAME: &str = "example_insert_test";
pub(crate) const ROWS: usize = 100_000;
pub(crate) const DEFAULT_RUNS: usize = 10;
const DISABLE_CLEANUP_ENV: &str = "DISABLE_CLEANUP";

pub(crate) fn init(directives: Option<&[(&str, &str)]>) {
    if let Ok(l) = std::env::var("RUST_LOG") {
        if !l.is_empty() {
            // Add directives here
            init_tracing(directives);
        }
    }
}

pub(crate) fn get_args() -> usize {
    // ENV first
    std::env::var("EXAMPLE_RUNS")
        .ok()
        .and_then(|e| e.parse::<usize>().ok())
        // Overriden by CLI
        .or(std::env::args().nth(1).and_then(|a| a.parse::<usize>().ok()))
        // Otherwise default
        .unwrap_or(DEFAULT_RUNS)
}

pub(crate) async fn setup(
    directives: Option<&[(&str, &str)]>,
) -> (&'static ClickHouseContainer, usize) {
    let num_runs = get_args();
    eprintln!("Running with # of runs = {num_runs}");

    // Init tracing
    init(directives);

    // Setup container
    let ch = get_or_create_container(None).await;

    (ch, num_runs)
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
    F: FnOnce(&'static ClickHouseContainer, usize) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    // Initialize container and tracing
    let (ch, num_runs) = setup(directives).await;
    let result = AssertUnwindSafe(example(ch, num_runs)).catch_unwind().await;

    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return result;
    }

    ch.shutdown().await.expect("Shutting down container");

    result
}

/// Test harness for ad-hoc examples
///
/// # Errors
/// # Panics
pub(crate) async fn run_example<F, Fut>(
    example: F,
    directives: Option<&[(&str, &str)]>,
) -> Result<(), Box<dyn std::any::Any + Send>>
where
    F: FnOnce(usize) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    // Initialize container and tracing
    let num_runs = get_args();

    // Init tracing
    init(directives);

    AssertUnwindSafe(example(num_runs)).catch_unwind().await
}

pub(crate) fn header(msg: impl std::fmt::Display) {
    eprintln!("--------\n{msg}\n--------");
}

pub(crate) fn timing(msg: impl std::fmt::Display, instant: Instant) {
    eprintln!("{msg} Time: {:#?}", instant.elapsed());
}
