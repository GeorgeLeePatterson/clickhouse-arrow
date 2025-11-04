## [0.2.0] - 2025-11-04

### Bug Fixes

- Resolve stack overflow and implement query parameters ([#52](https://github.com/georgeleepatterson/clickhouse-arrow/issues/52)) ([#71](https://github.com/georgeleepatterson/clickhouse-arrow/issues/71)) ([5f57e1e](https://github.com/georgeleepatterson/clickhouse-arrow/commit/5f57e1e637546ae568840fee6464e02201ca526a))

### Features

- Increase connection limit to 16, fix overflow bug ([#77](https://github.com/georgeleepatterson/clickhouse-arrow/issues/77)) ([013c06d](https://github.com/georgeleepatterson/clickhouse-arrow/commit/013c06dcb0de23e2ce7d8ea3364dd667b451f6c0))
- Add performance benchmark suite with tmpfs support ([#78](https://github.com/georgeleepatterson/clickhouse-arrow/issues/78)) ([58d543f](https://github.com/georgeleepatterson/clickhouse-arrow/commit/58d543f6e8f0b6125bfaa1643879cea2763f3388))

### Miscellaneous Tasks

- Patches justfile ([b1835a8](https://github.com/georgeleepatterson/clickhouse-arrow/commit/b1835a8767b6c0cc69b4376caf0988e858b9492e))
- Patches justfile release ([b988d65](https://github.com/georgeleepatterson/clickhouse-arrow/commit/b988d65da58ed5b864a270e8f475fe9fbc20d786))

### Build

- Bump the patch-updates group across 1 directory with 4 updates ([#80](https://github.com/georgeleepatterson/clickhouse-arrow/issues/80)) ([930c384](https://github.com/georgeleepatterson/clickhouse-arrow/commit/930c38435ae12d72847fe6349419550c9b717164))
- Bump rust_decimal from 1.37.2 to 1.39.0 ([#76](https://github.com/georgeleepatterson/clickhouse-arrow/issues/76)) ([bb5dd61](https://github.com/georgeleepatterson/clickhouse-arrow/commit/bb5dd61476ed18868d2adb7dbb76a37544eab603))
- Bump bytemuck from 1.23.2 to 1.24.0 ([#75](https://github.com/georgeleepatterson/clickhouse-arrow/issues/75)) ([6e28ab2](https://github.com/georgeleepatterson/clickhouse-arrow/commit/6e28ab2c1424bfdf893ec8638bcc5d4ff32b5a67))
- Bump clickhouse from 0.13.3 to 0.14.0 ([#74](https://github.com/georgeleepatterson/clickhouse-arrow/issues/74)) ([046eb1f](https://github.com/georgeleepatterson/clickhouse-arrow/commit/046eb1fa171ee96bba3308f0509ad12793de9f1c))
- Bump tokio from 1.47.1 to 1.48.0 ([#73](https://github.com/georgeleepatterson/clickhouse-arrow/issues/73)) ([0e638f3](https://github.com/georgeleepatterson/clickhouse-arrow/commit/0e638f34c1ac58d6a03c2bbfd014f02127ef9638))
- Patches bug where version missing in changelog ([b989a4e](https://github.com/georgeleepatterson/clickhouse-arrow/commit/b989a4e42fbad63ba67a7e3c06b6397dba25e9ba))

### Notable Contributions

@nazq - Special thanks to the contributions from @nazq. Most of the features added in this release come from those contributions. `0.2.0` is a jump up in quality and capabilities as a result.
