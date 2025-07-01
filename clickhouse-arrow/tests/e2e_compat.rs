#![allow(unused_crate_dependencies)]

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[("testcontainers", "debug")];

// Test arrow/native compat e2e
e2e_test!(e2e_compat, tests::compat::test_arrow_compat, TRACING_DIRECTIVES, None);
