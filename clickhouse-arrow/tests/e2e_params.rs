#![allow(unused_crate_dependencies)]

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[("testcontainers", "debug")];

// Test parameter functionality - basic string identifier (reproduces stack overflow bug from issue
// #52)
e2e_test!(
    e2e_params_basic_string_identifier,
    tests::params::test_params_basic_string_identifier,
    TRACING_DIRECTIVES,
    None
);

// Test parameter functionality - integers
e2e_test!(e2e_params_integer, tests::params::test_params_integer, TRACING_DIRECTIVES, None);

// Test parameter functionality - strings
e2e_test!(e2e_params_string, tests::params::test_params_string, TRACING_DIRECTIVES, None);

// Test parameter functionality - arrays (original feature request from issue #52)
e2e_test!(e2e_params_array_int32, tests::params::test_params_array_int32, TRACING_DIRECTIVES, None);

// Test parameter functionality - mixed types
e2e_test!(e2e_params_mixed_types, tests::params::test_params_mixed_types, TRACING_DIRECTIVES, None);
