# Contributing to clickhouse-arrow

Thank you for your interest in contributing to clickhouse-arrow! This document provides guidelines and instructions for contributing to the project.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Create a new branch for your feature or bug fix
4. Make your changes
5. Run tests and ensure they pass
6. Submit a pull request

## Development Setup

### Prerequisites

- Rust 1.70+ (we use the 2024 edition)
- Docker (for running integration tests)
- ClickHouse server (optional for manual testing)

### Building

```bash
# Build the project
cargo build

# Build with release optimizations
cargo build --release
```

### Testing

```bash
# Run all tests (requires Docker for integration tests)
just test
# Or
cargo test --features test_utils

# Run specific test suites
just test-one e2e_arrow
# Or
cargo test --test e2e_arrow --features test_utils
cargo test --test e2e_native --features test_utils

# Run with output visible
cargo test --features test_utils -- --nocapture
```

### Code Quality

Before submitting a PR, please ensure:

```bash
# Format code
cargo fmt

# Run clippy
cargo clippy --all-features --all-targets

# Run against all features if feature specific work was added
just check-features

# Check for security issues
cargo audit
```

## Guidelines

### Code Style

- Follow Rust standard formatting (use `cargo fmt`, pedantic linting is used)
- Write clear, self-documenting code
- Add documentation comments for public APIs
- Include examples in documentation where appropriate

### Commit Messages

- Use clear, descriptive commit messages
- Follow conventional commit format when possible:
  - `feat:` for new features
  - `fix:` for bug fixes
  - `docs:` for documentation changes
  - `test:` for test additions/changes
  - `refactor:` for code refactoring
  - `perf:` for performance improvements

### Pull Requests

1. **Keep PRs focused**: One feature or fix per PR
2. **Write tests**: Include tests for new functionality
3. **Update documentation**: Keep docs in sync with code changes
4. **Add examples**: For significant features, add examples
5. **Benchmark if needed**: For performance-critical changes

### Testing

- Unit tests go inline with modules
- Integration tests use Docker containers (via testcontainers)
- All tests must pass before merging
- Aim for high test coverage (currently at 90%+)

## Architecture Overview

Key components:

- **Client Module** (`src/client/`): Connection management and query execution
- **Protocol Implementation** (`src/native/`): ClickHouse wire protocol
- **Data Formats**:
  - Entrypoint (`src/formats/`): Internal type system
  - Arrow Format (`src/formats/arrow/`, `src/arrow/`): Arrow RecordBatch integration
  - Native Format (`src/formats/native/`, `src/native/`): Internal type system
- **Type System** (`src/native/types/`, `src/arrow/types/`): Comprehensive ClickHouse type support

## Reporting Issues

When reporting issues, please include:

- Rust version (`rustc --version`)
- ClickHouse server version
- Minimal reproducible example
- Error messages and stack traces
- Expected vs actual behavior

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas
- Check existing issues before creating new ones

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
