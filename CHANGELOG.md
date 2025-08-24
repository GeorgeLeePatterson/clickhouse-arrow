# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Documentation

- Fixes broken doc links, inlines readme ([#35](https://github.com/georgeleepatterson/clickhouse-arrow/issues/35)) ([81c360a](https://github.com/georgeleepatterson/clickhouse-arrow/commit/81c360a19dad0b7e745feb4cc130bfb5a1219067))

### Miscellaneous Tasks

- Updates release workflow, dry run ([#31](https://github.com/georgeleepatterson/clickhouse-arrow/issues/31)) ([8a7a6b1](https://github.com/georgeleepatterson/clickhouse-arrow/commit/8a7a6b19b7ccff86387eef5d5770b88e9cdbf039))
- Updates release workflow to include version ([#32](https://github.com/georgeleepatterson/clickhouse-arrow/issues/32)) ([55f8324](https://github.com/georgeleepatterson/clickhouse-arrow/commit/55f8324781f67f2af6c24d41d27b3971b4b43529))
- Patches typo in variable for release ([#33](https://github.com/georgeleepatterson/clickhouse-arrow/issues/33)) ([06d69d2](https://github.com/georgeleepatterson/clickhouse-arrow/commit/06d69d256c322228b2e58be38b4a26db4ef2ac07))

## [0.1.5] - 2025-08-13

### Bug Fixes

- Upgrades protocol revision to latest, 54479 ([#28](https://github.com/georgeleepatterson/clickhouse-arrow/issues/28)) ([4eac72d](https://github.com/georgeleepatterson/clickhouse-arrow/commit/4eac72d38a72542c066520cc64f8199a3f4a6b79))

### Documentation

- Patches formatting in README for notes ([#25](https://github.com/georgeleepatterson/clickhouse-arrow/issues/25)) ([333b43c](https://github.com/georgeleepatterson/clickhouse-arrow/commit/333b43c0060c4ce82f09a2a795b66f4ffa68df73))
- Updates links in README ([42fc74f](https://github.com/georgeleepatterson/clickhouse-arrow/commit/42fc74f1ca12959566599af2eed9e85ceca00a26))
- Removes extra keyword ([#30](https://github.com/georgeleepatterson/clickhouse-arrow/issues/30)) ([86add54](https://github.com/georgeleepatterson/clickhouse-arrow/commit/86add5433fe8823ac50b31a6cdf86ce79710b2e6))

### Miscellaneous Tasks

- Updates pr and labeler ([#22](https://github.com/georgeleepatterson/clickhouse-arrow/issues/22)) ([27e5b66](https://github.com/georgeleepatterson/clickhouse-arrow/commit/27e5b66ff06639f58790976644595c2a24832374))
- Updates ci workflow to prevent double fmt checks ([#24](https://github.com/georgeleepatterson/clickhouse-arrow/issues/24)) ([d16effe](https://github.com/georgeleepatterson/clickhouse-arrow/commit/d16effec900b8ae8cb4ab113be7a8e7625b030ef))
- Prepare release v0.1.5 ([#29](https://github.com/georgeleepatterson/clickhouse-arrow/issues/29)) ([951a1df](https://github.com/georgeleepatterson/clickhouse-arrow/commit/951a1dfa8e2c2a6ec85d0bfcdcc2ff36a76d5e96))

### Testing

- Updates benchmarks to use ipv4 only, includes results in README ([#26](https://github.com/georgeleepatterson/clickhouse-arrow/issues/26)) ([c7ba6de](https://github.com/georgeleepatterson/clickhouse-arrow/commit/c7ba6de060e0eab38f4ec70731254ba403677871))

## [0.1.4] - 2025-07-30

### Miscellaneous Tasks

- Prepare release v0.1.4 ([#20](https://github.com/georgeleepatterson/clickhouse-arrow/issues/20)) ([cecdf05](https://github.com/georgeleepatterson/clickhouse-arrow/commit/cecdf050318586e53198ad5506c01f402e5e2810))

### Testing

- Patches mishandling of RUST_LOG env in test_utils ([#15](https://github.com/georgeleepatterson/clickhouse-arrow/issues/15)) ([e88b023](https://github.com/georgeleepatterson/clickhouse-arrow/commit/e88b023531cda1f8a3c94c0536abc9769da14536))

## [0.1.3] - 2025-07-05

### Bug Fixes

- Ergonomics ([#12](https://github.com/georgeleepatterson/clickhouse-arrow/issues/12)) ([d6ec132](https://github.com/georgeleepatterson/clickhouse-arrow/commit/d6ec13277f9532ae877e6508b088e5d0af0aa3b9))
- Adds clickhouse engines to schema ([450e559](https://github.com/georgeleepatterson/clickhouse-arrow/commit/450e5592b28dcb911c4b0d46481ecb034bd1c881))

### Miscellaneous Tasks

- Test cleanup, renames test-utils feature ([#13](https://github.com/georgeleepatterson/clickhouse-arrow/issues/13)) ([f02d73a](https://github.com/georgeleepatterson/clickhouse-arrow/commit/f02d73abe352a10a3318ed81839518fae23afa1d))
- Prepare release v0.1.3 ([#14](https://github.com/georgeleepatterson/clickhouse-arrow/issues/14)) ([ca42b58](https://github.com/georgeleepatterson/clickhouse-arrow/commit/ca42b5881a98d3100b782b16db34b1a1796c63be))

### Testing

- Adds coverage for clickhouse engines ([901bfe4](https://github.com/georgeleepatterson/clickhouse-arrow/commit/901bfe4b7ab3b38445f795484ff54820043c05a3))

## [0.1.2] - 2025-07-02

### Miscellaneous Tasks

- Updates to release workflow and test coverage ([#8](https://github.com/georgeleepatterson/clickhouse-arrow/issues/8)) ([ce66a78](https://github.com/georgeleepatterson/clickhouse-arrow/commit/ce66a78c3f95cc4a6a62afd1146fd9bc8af07786))
- Release v0.1.2 ([#9](https://github.com/georgeleepatterson/clickhouse-arrow/issues/9)) ([ba5710c](https://github.com/georgeleepatterson/clickhouse-arrow/commit/ba5710c34836bef2a8b148f2246b3682a6c5f1da))

## [0.1.1] - 2025-07-01

### Bug Fixes

- Refactors internal, provides additional benches, more tests ([8b82b95](https://github.com/georgeleepatterson/clickhouse-arrow/commit/8b82b956657185da5ac6820f8cb2e3c0b707213f))
- Removes row binary experiment, cleans up code, adds typed builders, sync deser ([3c547af](https://github.com/georgeleepatterson/clickhouse-arrow/commit/3c547af7ad7287f4800e737240da10995bff3c6a))
- Fixes lints for 1.88 ([09fa8f5](https://github.com/georgeleepatterson/clickhouse-arrow/commit/09fa8f58e9bc3d57b9751c30769b76f02f6e7e08))
- Lints in derive ([26f9d8d](https://github.com/georgeleepatterson/clickhouse-arrow/commit/26f9d8d6e9a374caf459103dca159a8884760565))

### Documentation

- Updates docs ([626eb39](https://github.com/georgeleepatterson/clickhouse-arrow/commit/626eb3969a0b1333f59e1d82e564940c30ad63e8))
- Adds readme to derive ([97ff54e](https://github.com/georgeleepatterson/clickhouse-arrow/commit/97ff54efbcbdf3a476f2360d9d8eb14ed15eb0b5))
- Updated cargo toml ([320c3cc](https://github.com/georgeleepatterson/clickhouse-arrow/commit/320c3cc62420760861151eae0109c70b746002f0))
- Removed typo in docs ([3a77b13](https://github.com/georgeleepatterson/clickhouse-arrow/commit/3a77b13588d4d6c3df50a8540b2ba511765db32c))

### Features

- Adds 'fast_mode', to be renamed, with inner pool, created examples and benches ([5833ba2](https://github.com/georgeleepatterson/clickhouse-arrow/commit/5833ba232e47e74ca5bfeee236d528d3c13ab68d))
- Adds rowbinary ([6130637](https://github.com/georgeleepatterson/clickhouse-arrow/commit/6130637d4184ecb176c21388c97b5a58cd9a55f4))

### Miscellaneous Tasks

- Renames to clickhouse-arrow ([8b0a69e](https://github.com/georgeleepatterson/clickhouse-arrow/commit/8b0a69efcaeb865c9df5e05adcb2e6e97a9d2b6e))
- Addresses formatting ([5358b38](https://github.com/georgeleepatterson/clickhouse-arrow/commit/5358b38b930284fa709913475c977fbdd9af8e3c))
- Addresses formatting in integration tests ([2ee85b3](https://github.com/georgeleepatterson/clickhouse-arrow/commit/2ee85b323dca02c9e58fe896d509859185ea25ef))
- Additional work around lints, mainly to satisfy nightly around let chains ([7ebcc34](https://github.com/georgeleepatterson/clickhouse-arrow/commit/7ebcc34c269cf7aec744c974b473a6fe198dde08))
- Some lints, cleanup, example cleanup, and bench cleanup ([2cd54fb](https://github.com/georgeleepatterson/clickhouse-arrow/commit/2cd54fb1f94b9ccf3bdff8a739fe77c5f793e599))
- Add codecov token ([c89712a](https://github.com/georgeleepatterson/clickhouse-arrow/commit/c89712a067a0700f186042890b65040455df9ea1))
- Update release configuration and prepare for 0.1.0 ([31f88b4](https://github.com/georgeleepatterson/clickhouse-arrow/commit/31f88b4ddf2d7603de7895c087bcd4ebcda58b47))
- Updates release.toml ([11d7dba](https://github.com/georgeleepatterson/clickhouse-arrow/commit/11d7dbae48dd77496969c33cafd4ca8b3dffdfbb))
- Trying to get release.toml right ([d22cc2d](https://github.com/georgeleepatterson/clickhouse-arrow/commit/d22cc2dde3b66cb58cbcf1a93594c59ce811c390))
- Release toml hopefully working now ([31d6d8b](https://github.com/georgeleepatterson/clickhouse-arrow/commit/31d6d8b91f930cc11e67aeccef63d1997cfe6b90))
- Updates derive readme, updates release toml ([cf54afd](https://github.com/georgeleepatterson/clickhouse-arrow/commit/cf54afddf48a04a44bd0bdba297cee70f17c3937))
- Trying to get release.toml right ([e08b6d4](https://github.com/georgeleepatterson/clickhouse-arrow/commit/e08b6d44fa5a8f48453183c625189a138b05f554))
- Trying to get release.toml right ([efd0f73](https://github.com/georgeleepatterson/clickhouse-arrow/commit/efd0f7392dc2821e5c111145204c77ebd799f7ac))
- Trying to get release.toml right ([8693618](https://github.com/georgeleepatterson/clickhouse-arrow/commit/869361865dfc24b395e6a24a271370ccfd5cccfa))
- Trying to get release.toml right ([9094274](https://github.com/georgeleepatterson/clickhouse-arrow/commit/9094274fbb350c8c317c0e2dee2f2ad5a6fe2357))
- Release v0.1.1 ([626dc6e](https://github.com/georgeleepatterson/clickhouse-arrow/commit/626dc6e791ce5fc666c1f8ee34be110e704c3d04))

### Testing

- Increasing test coverage ([e634f1f](https://github.com/georgeleepatterson/clickhouse-arrow/commit/e634f1fae6791515d66180b06d1bc4d37d7986f4))
- 90% line coverage ([683e42f](https://github.com/georgeleepatterson/clickhouse-arrow/commit/683e42f5f5815861474d8ea142adbb70b1f3f8fb))

### Build

- Updates ci to use nightly rustfmt ([272f62b](https://github.com/georgeleepatterson/clickhouse-arrow/commit/272f62bb037408032174efbafd5f3e9594634469))
- Updates ci to install clippy for nightly ([8217724](https://github.com/georgeleepatterson/clickhouse-arrow/commit/8217724dfded6b3453e30261841fa57ae521b087))
- Working through getting the ci workflow right ([784a4da](https://github.com/georgeleepatterson/clickhouse-arrow/commit/784a4da7527f4ec73a561653fed9a756817d1315))


