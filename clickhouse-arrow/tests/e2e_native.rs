#![allow(unused_crate_dependencies)]

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[("testcontainers", "debug")];

// Test native e2e
#[cfg(feature = "derive")]
e2e_test!(e2e_native, tests::native::test_round_trip, TRACING_DIRECTIVES, None);
