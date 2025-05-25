pub mod arrow_helpers;
pub mod constants;
pub mod native_helpers;

use std::sync::Arc;

use clickhouse_native::test_utils::{
    ClickHouseContainer, get_or_create_container_multi, init_tracing,
};

use crate::tests::TESTS_RUNNING;

pub const SEP: &str = "\n-------------------------------\n";

/// Little helper function to print headers for tests
pub fn header(qid: impl std::fmt::Display, msg: impl AsRef<str>) {
    eprintln!("{SEP} Query ID = {qid}\n {} {SEP}", msg.as_ref());
}

/// Initialize traacing and get thread safe reference to `ClickHouseContainer`
pub async fn init(
    name: &str,
    directives: Option<&[(&str, &str)]>,
) -> &'static Arc<ClickHouseContainer> {
    init_tracing(directives);
    get_or_create_container_multi(name, TESTS_RUNNING.as_ref()).await
}
