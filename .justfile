LOG := env('RUST_LOG', '')
ARROW_DEBUG := env('CLICKHOUSE_NATIVE_DEBUG_ARROW', '')
DISABLE_CLEANUP := env('DISABLE_CLEANUP', '')

# List of features
# features := ["inner_pool", "pool", "serde", "derive", "cloud", "rust_decimal"]
features := 'inner_pool pool serde derive cloud rust_decimal'

# List of Examples

examples := "insert insert_multi_threaded pool scalar"

default:
    @just --list

# --- TESTS ---
test:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils -- --nocapture --show-output

test-one test_name:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils "{{ test_name }}" -- --nocapture --show-output

test-integration test_name:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils --test "{{ test_name }}" -- --nocapture --show-output

coverage:
    cargo llvm-cov --html \
     --ignore-filename-regex "(clickhouse-arrow-derive|errors|error_codes|examples|test_utils).*" \
     --output-dir coverage -F test-utils --open

# --- COVERAGE ---
coverage-json:
    cargo llvm-cov --json \
     --ignore-filename-regex "(clickhouse-arrow-derive|errors|error_codes|examples|test_utils).*" \
     --output-path coverage/cov.json -F test-utils

coverage-lcov:
    cargo llvm-cov --lcov \
     --ignore-filename-regex "(clickhouse-arrow-derive|errors|error_codes|examples|test_utils).*" \
     --output-path coverage/lcov.info -F test-utils

# --- DOCS ---
docs:
    cd clickhouse-arrow && cargo doc --open

# --- BENCHES ---
[confirm('Delete all benchmark reports?')]
clear-benches:
    rm -rf target/criterion/*

bench:
    cd clickhouse-arrow && RUST_LOG={{ LOG }} DISABLE_CLEANUP={{ DISABLE_CLEANUP }} cargo bench \
    --profile=release -F test-utils && open ../target/criterion/report/index.html

bench-lto:
    cd clickhouse-arrow && RUST_LOG={{ LOG }} DISABLE_CLEANUP={{ DISABLE_CLEANUP }} cargo bench \
    --profile=release-lto -F test-utils && open ../target/criterion/report/index.html

bench-lto-update:
    cd clickhouse-arrow && \
     DISABLE_CLEANUP={{ DISABLE_CLEANUP }} cargo bench --profile=release-lto -F test-utils --bench "scalar" && \
     DISABLE_CLEANUP={{ DISABLE_CLEANUP }} cargo bench --profile=release-lto -F test-utils --bench "insert"

bench-one bench:
    cd clickhouse-arrow && RUST_LOG={{ LOG }} DISABLE_CLEANUP={{ DISABLE_CLEANUP }} cargo bench \
     --profile=release \
     -F test-utils \
     --bench "{{ bench }}" && \
     open ../target/criterion/report/index.html

bench-one-lto bench:
    cd clickhouse-arrow && RUST_LOG={{ LOG }} DISABLE_CLEANUP={{ DISABLE_CLEANUP }} cargo bench \
     --profile=release-lto \
     -F test-utils \
     --bench "{{ bench }}" && \
     open ../target/criterion/report/index.html

# --- EXAMPLES ---
debug-profile example:
    cd clickhouse-arrow && RUSTFLAGS='-g' cargo build \
     -F test-utils \
     --example "{{ example }}"

release-debug example:
    cd clickhouse-arrow && RUSTFLAGS='-g' cargo build \
     --profile=release-with-debug \
     -F test-utils \
     --example "{{ example }}"
    codesign -s - -v -f --entitlements assets/mac.entitlements "target/release-with-debug/examples/{{ example }}"

release-lto example:
    cd clickhouse-arrow && cargo build \
     --profile=release-lto \
     -F test-utils \
     --example "{{ example }}"
    codesign -s - -v -f --entitlements assets/mac.entitlements "target/release-lto/examples/{{ example }}"

example example:
    cargo run -F test-utils --example "{{ example }}"

example-lto example:
    cargo run --profile=release-lto -F test-utils --example "{{ example }}"

example-release-debug example:
    cargo run --profile=release-with-debug -F test-utils --example "{{ example }}"

examples:
    @for ex in {{ examples }}; do \
        echo "Running example: $ex"; \
        cargo run -F test-utils --example "$ex"; \
    done

# --- PROFILING ---
flamegraph example *args='':
    CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --root --flamechart --open \
     --profile=release-with-debug \
     --features test-utils \
     --min-width="0.0001" \
     --example "{{ example }}" -- "{{ args }}"

samply example *args='': (release-debug example)
    # TODO: Add install check here
    samply record -r 100000 \
     "target/release-with-debug/examples/{{ example }}" "{{ args }}"

# --- CLIPPY AND FORMATTING ---

# Check all feature combinations
check-features *ARGS=features:
    @echo "Checking no features..."
    cargo clippy --no-default-features --all-targets
    @echo "Building no features..."
    cargo check --no-default-features --all-targets
    @echo "Checking default features..."
    cargo clippy --all-targets
    @echo "Building default features..."
    cargo check --all-targets
    @echo "Checking all features..."
    cargo clippy --all-features --all-targets
    @echo "Building all features..."
    cargo check --all-features --all-targets
    @echo "Checking each feature..."
    @for feature in {{ ARGS }}; do \
        echo "Checking & Building feature: $feature"; \
        cargo clippy --no-default-features --features $feature --all-targets; \
        cargo check --no-default-features --features $feature --all-targets; \
    done
    @echo "Checking each feature with defaults..."
    @for feature in {{ ARGS }}; do \
        echo "Checking feature (with defaults): $feature"; \
        cargo clippy --features $feature --all-targets; \
        cargo check --features $feature --all-targets; \
    done
    @echo "Checking all provided features..."
    cargo clippy --no-default-features --features "{{ ARGS }}" --all-targets
    cargo check --no-default-features --features "{{ ARGS }}" --all-targets

fmt:
    @echo "Running rustfmt..."
    # cd clickhouse-arrow && cargo +nightly fmt --all --check
    cargo +nightly fmt --check -- --config-path ./rustfmt.toml
fix:
    cargo clippy --fix --all-features --all-targets --allow-dirty

# --- MAINTENANCE ---

# Run checks CI will
checks:
    cargo +nightly fmt -- --check
    cargo +nightly clippy --all-features --all-targets
    cargo +stable clippy --all-features --all-targets -- -D warnings
    just -f {{justfile()}} test

# Initialize development environment for maintainers
init-dev:
    @echo "Installing development tools..."
    cargo install cargo-release || true
    cargo install git-cliff || true
    cargo install cargo-edit || true
    cargo install cargo-outdated || true
    cargo install cargo-audit || true
    @echo ""
    @echo "✅ Development tools installed!"
    @echo ""
    @echo "Next steps:"
    @echo "1. Get your crates.io API token from https://crates.io/settings/tokens"
    @echo "2. Add it as CARGO_REGISTRY_TOKEN in GitHub repo settings → Secrets"
    @echo "3. Use 'cargo release patch/minor/major' to create releases"
    @echo ""
    @echo "Useful commands:"
    @echo "  just release-dry patch  # Preview what would happen"
    @echo "  just check-outdated     # Check for outdated dependencies"
    @echo "  just audit              # Security audit"

# Check for outdated dependencies
check-outdated:
    cargo outdated

# Run security audit
audit:
    cargo audit

# Prepare a release (creates PR with version bumps and changelog)
prepare-release version:
    #!/usr/bin/env bash
    set -euo pipefail

    # Validate version format
    if ! [[ "{{version}}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "Error: Version must be in format X.Y.Z (e.g., 0.2.0)"
        exit 1
    fi

    # Parse version components
    IFS='.' read -r MAJOR MINOR PATCH <<< "{{version}}"

    # Get current version for release notes
    CURRENT_VERSION=$(grep -E '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')

    # Create release branch
    git checkout -b "release-v{{version}}"

    # Update workspace version in root Cargo.toml (only in [workspace.package] section)
    # This uses a more specific pattern to only match the version under [workspace.package]
    awk '/^\[workspace\.package\]/ {in_workspace=1} in_workspace && /^version = / {gsub(/"[^"]*"/, "\"{{version}}\""); in_workspace=0} {print}' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml

    # Update version constants in constants.rs
    sed -i '' "s/pub(super) const VERSION_MAJOR: u64 = [0-9]*;/pub(super) const VERSION_MAJOR: u64 = $MAJOR;/" clickhouse-arrow/src/constants.rs
    sed -i '' "s/pub(super) const VERSION_MINOR: u64 = [0-9]*;/pub(super) const VERSION_MINOR: u64 = $MINOR;/" clickhouse-arrow/src/constants.rs
    sed -i '' "s/pub(super) const VERSION_PATCH: u64 = [0-9]*;/pub(super) const VERSION_PATCH: u64 = $PATCH;/" clickhouse-arrow/src/constants.rs

    # Update clickhouse-arrow version references in README files (if they exist)
    # Look for patterns like: clickhouse-arrow = "0.1.1" or clickhouse-arrow = { version = "0.1.1"
    for readme in README.md clickhouse-arrow/README.md; do
        if [ -f "$readme" ]; then
            # Update simple dependency format
            sed -i '' "s/clickhouse-arrow = \"[0-9]*\.[0-9]*\.[0-9]*\"/clickhouse-arrow = \"{{version}}\"/" "$readme" || true
            # Update version field in dependency table format
            sed -i '' "s/clickhouse-arrow = { version = \"[0-9]*\.[0-9]*\.[0-9]*\"/clickhouse-arrow = { version = \"{{version}}\"/" "$readme" || true
        fi
    done

    # Update Cargo.lock
    cargo update --workspace

    # Run version test to verify
    echo "Verifying version consistency..."
    cargo test test_version_matches_cargo --features test-utils

    # Generate full changelog
    echo "Generating changelog..."
    git cliff --tag v{{version}} -o CHANGELOG.md

    # Generate release notes for this version
    echo "Generating release notes..."
    git cliff --unreleased --tag v{{version}} --strip header -o RELEASE_NOTES.md

    # Stage all changes
    git add Cargo.toml clickhouse-arrow/src/constants.rs Cargo.lock CHANGELOG.md RELEASE_NOTES.md
    # Also add README files if they were modified
    git add README.md clickhouse-arrow/README.md 2>/dev/null || true

    # Commit
    git commit -m "chore: prepare release v{{version}}"

    # Push branch
    git push origin "release-v{{version}}"

    echo ""
    echo "✅ Release preparation complete!"
    echo ""
    echo "Release notes preview:"
    echo "----------------------"
    head -20 RELEASE_NOTES.md
    echo ""
    echo "Next steps:"
    echo "1. Create a PR from the 'release-v{{version}}' branch"
    echo "2. Review and merge the PR"
    echo "3. After merge, run: just tag-release {{version}}"
    echo ""

# Tag a release after the PR is merged
tag-release version:
    #!/usr/bin/env bash
    set -euo pipefail

    # Ensure we're on main and up to date
    git checkout main
    git pull origin main

    # Verify the version in Cargo.toml matches
    CARGO_VERSION=$(grep -E '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    if [ "$CARGO_VERSION" != "{{version}}" ]; then
        echo "Error: Cargo.toml version ($CARGO_VERSION) does not match requested version ({{version}})"
        echo "Did the release PR merge successfully?"
        exit 1
    fi

    # Verify publish will work
    cargo publish --dry-run -p clickhouse-arrow-derive --no-verify
    sleep 10 # Wait for crates.io to index
    cargo publish --dry-run -p clickhouse-arrow --no-verify

    # Create and push tag
    git tag -a "v{{version}}" -m "Release v{{version}}"
    git push origin "v{{version}}"

    echo ""
    echo "✅ Tag v{{version}} created and pushed!"
    echo "The release workflow will now run automatically."
    echo ""

# Preview what a release would do (dry run)
release-dry version:
    @echo "This would:"
    @echo "1. Create branch: release-v{{version}}"
    @echo "2. Update version to {{version}} in:"
    @echo "   - Cargo.toml (workspace.package section only)"
    @echo "   - clickhouse-arrow/src/constants.rs"
    @echo "   - README files (if they contain clickhouse-arrow version references)"
    @echo "3. Update Cargo.lock"
    @echo "4. Generate CHANGELOG.md"
    @echo "5. Generate RELEASE_NOTES.md"
    @echo "6. Create commit and push branch"
    @echo ""
    @echo "After PR merge, 'just tag-release {{version}}' would:"
    @echo "1. Tag the merged commit as v{{version}}"
    @echo "2. Push the tag (triggering release workflow)"
