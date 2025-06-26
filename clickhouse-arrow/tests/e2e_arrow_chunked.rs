#![allow(unused_crate_dependencies)]

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[("testcontainers", "debug")];
const CONF: &str = "config_chunked.xml";

// Test arrow e2e lz4
e2e_test!(
    e2e_chunked_arrow_none,
    tests::arrow::test_round_trip_none,
    TRACING_DIRECTIVES,
    Some(CONF)
);

// Test arrow e2e lz4
e2e_test!(e2e_chunked_arrow_lz4, tests::arrow::test_round_trip_lz4, TRACING_DIRECTIVES, Some(CONF));

// Test arrow e2e zstd
e2e_test!(
    e2e_chunked_arrow_zstd,
    tests::arrow::test_round_trip_zstd,
    TRACING_DIRECTIVES,
    Some(CONF)
);

// Test arrow schema utils
e2e_test!(
    e2e_chunked_arrow_schema,
    tests::arrow::test_schema_utils,
    TRACING_DIRECTIVES,
    Some(CONF)
);

// Test arrow execute scalar/settings
e2e_test!(
    e2e_chunked_arrow_execute,
    tests::arrow::test_execute_queries,
    TRACING_DIRECTIVES,
    Some(CONF)
);
