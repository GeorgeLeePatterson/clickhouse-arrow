#![allow(unused_crate_dependencies)]

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[("testcontainers", "debug")];

// // Test arrow e2e
// e2e_test!(e2e_arrow, tests::arrow::test_round_trip, TRACING_DIRECTIVES);

// Test native e2e
e2e_test!(e2e_native, tests::native::test_round_trip, TRACING_DIRECTIVES);
