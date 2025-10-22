pub(crate) mod scale_utils;

use std::panic::AssertUnwindSafe;

use clickhouse_arrow::test_utils::{
    ClickHouseContainer, get_or_create_benchmark_container, get_or_create_container, init_tracing,
};
use futures_util::FutureExt;

#[allow(unused)]
pub(crate) const DB_NAME: &str = "example_insert_test";
#[allow(unused)]
pub(crate) const ROWS: usize = 100_000;
const DISABLE_CLEANUP_ENV: &str = "DISABLE_CLEANUP";

/// Print a banner with dynamic width based on content
///
/// # Example
/// ```
/// print_banner("Large Scale Insert Test", None);
/// ```
/// Outputs:
/// ```text
/// ╔════════════════════════════════════════╗
/// ║  Large Scale Insert Test               ║
/// ╚════════════════════════════════════════╝
/// ```
#[allow(unused)]
pub(crate) fn print_banner(text: &str, width: Option<usize>) {
    let content_width = width.unwrap_or_else(|| text.len() + 4); // Default: text + 2 spaces padding on each side

    let top = format!("╔{}╗", "═".repeat(content_width));
    let bottom = format!("╚{}╝", "═".repeat(content_width));

    // Calculate padding for centered text
    let text_len = text.len();
    let total_padding = content_width.saturating_sub(text_len);
    let left_padding = total_padding / 2;
    let right_padding = total_padding - left_padding;

    let middle = format!("║{}{}{}║", " ".repeat(left_padding), text, " ".repeat(right_padding));

    eprintln!("{top}");
    eprintln!("{middle}");
    eprintln!("{bottom}");
}

pub(crate) fn init(directives: Option<&[(&str, &str)]>) {
    if let Ok(l) = std::env::var("RUST_LOG")
        && !l.is_empty()
    {
        // Add directives here
        init_tracing(directives);
    }
}

#[allow(dead_code)] // Used by non-benchmark examples
pub(crate) async fn setup(directives: Option<&[(&str, &str)]>) -> &'static ClickHouseContainer {
    // Init tracing
    init(directives);
    // Setup container
    get_or_create_container(None).await
}

/// Setup benchmark container with optional tmpfs for zero disk I/O
///
/// Tmpfs can be enabled by setting the `USE_TMPFS` environment variable to "true" or "1".
/// Without tmpfs (default), data is written to disk with normal Docker volume behavior.
///
/// # Environment Variables
/// - `USE_TMPFS`: Set to "true" or "1" to enable tmpfs mounts (default: false)
#[allow(dead_code)] // Available for tmpfs benchmarks
pub(crate) async fn setup_benchmark(
    directives: Option<&[(&str, &str)]>,
) -> &'static ClickHouseContainer {
    // Init tracing
    init(directives);
    // Setup container (tmpfs enabled if USE_TMPFS=true)
    get_or_create_benchmark_container(None).await
}

/// Test harness for catching panics and attempting to shutdown the container
///
/// # Errors
/// # Panics
#[allow(dead_code)] // Used by non-benchmark examples
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

/// Test harness for benchmarks with optional tmpfs-enabled container
///
/// Same as `run_example_with_cleanup` but can use tmpfs for zero disk I/O
/// when the `USE_TMPFS` environment variable is set to "true" or "1".
///
/// # Environment Variables
/// - `USE_TMPFS`: Set to "true" or "1" to enable tmpfs mounts (default: false)
///
/// # Errors
/// # Panics
#[allow(dead_code)] // Available for tmpfs benchmarks
pub(crate) async fn run_benchmark_with_cleanup<F, Fut>(
    example: F,
    directives: Option<&[(&str, &str)]>,
) -> Result<(), Box<dyn std::any::Any + Send>>
where
    F: FnOnce(&'static ClickHouseContainer) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    // Initialize benchmark container with tmpfs and tracing
    let ch = setup_benchmark(directives).await;
    let result = AssertUnwindSafe(example(ch)).catch_unwind().await;
    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return result;
    }
    ch.shutdown().await.expect("Shutting down container");
    result
}
