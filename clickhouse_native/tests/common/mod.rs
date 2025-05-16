pub mod arrow_helpers;
pub mod constants;
pub mod docker;
pub mod native_helpers;

use std::sync::Arc;

use tracing::debug;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

pub const SEP: &str = "\n-------------------------------\n";

/// Little helper function to print headers for tests
pub fn header(qid: impl std::fmt::Display, msg: impl AsRef<str>) {
    eprintln!("{SEP} Query ID = {qid}\n {} {SEP}", msg.as_ref());
}

/// Initialize traacing and get thread safe reference to `ClickHouseContainer`
pub async fn init(
    name: &str,
    directives: Option<&[(&str, &str)]>,
) -> &'static Arc<docker::ClickHouseContainer> {
    init_tracing(directives);
    docker::get_clickhouse_container(name).await
}

pub fn init_tracing(directives: Option<&[(&str, &str)]>) {
    let stdio_logger = tracing_subscriber::fmt::Layer::default()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_filter(get_filter(directives));

    // Initialize only if not already set (avoids multiple subscribers in tests)
    if tracing::subscriber::set_global_default(tracing_subscriber::registry().with(stdio_logger))
        .is_ok()
    {
        debug!(
            "Tracing initialized with RUST_LOG={}",
            std::env::var("RUST_LOG").unwrap_or_default()
        );
    }
}

/// # Panics
#[allow(unused)]
pub fn get_filter(directives: Option<&[(&str, &str)]>) -> EnvFilter {
    let mut filter = EnvFilter::new(LevelFilter::TRACE.to_string())
        .add_directive("datafusion=info".parse().unwrap())
        .add_directive("sqlparser=error".parse().unwrap())
        .add_directive("ureq=info".parse().unwrap())
        .add_directive("tonic=error".parse().unwrap())
        .add_directive("tower=error".parse().unwrap())
        .add_directive("tokio=info".parse().unwrap())
        .add_directive("runtime=error".parse().unwrap())
        .add_directive("hyper=error".parse().unwrap())
        .add_directive("h2=error".parse().unwrap())
        .add_directive("reqwest=error".parse().unwrap())
        .add_directive("rustls=error".parse().unwrap())
        .add_directive("opentelemetry_sdk=off".parse().unwrap());

    if let Some(directives) = directives {
        for (key, value) in directives {
            filter = filter.add_directive(format!("{key}={value}").parse().unwrap());
        }
    }

    filter
}
