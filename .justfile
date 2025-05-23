set positional-arguments := true

LOG := env_var_or_default('RUST_LOG', 'debug')
BACKTRACE := env_var_or_default('RUST_BACKTRACE', '1')
ARROW_DEBUG := env_var_or_default('CLICKHOUSE_NATIVE_DEBUG_ARROW', '')

default:
    just --list


test:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --release -F tests -- --nocapture --show-output

coverage:
    cargo llvm-cov -r --html \
     --ignore-filename-regex "(clickhouse_native_derive|error_codes).*" \
     --output-dir coverage -F tests --open
