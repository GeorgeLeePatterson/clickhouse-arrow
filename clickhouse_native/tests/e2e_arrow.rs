#![allow(unused_crate_dependencies)]

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[("testcontainers", "debug")];

// Test arrow e2e
e2e_test!(e2e_arrow, tests::arrow::test_round_trip, TRACING_DIRECTIVES);

// Test arrow schema utils
e2e_test!(e2e_arrow_schema, tests::arrow::test_schema_utils, TRACING_DIRECTIVES);

// Test arrow execute scalar/settings
e2e_test!(e2e_arrow_execute, tests::arrow::test_execute_queries, TRACING_DIRECTIVES);
