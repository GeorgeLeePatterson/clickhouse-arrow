pub(super) const VERSION_MAJOR: u64 = 0;
pub(super) const VERSION_MINOR: u64 = 4;
pub(super) const VERSION_PATCH: u64 = 0;

// 8MB send and receive buffer sizes
pub(super) const TCP_READ_BUFFER_SIZE: u32 = 16 * 1024; // 16KB
pub(super) const TCP_WRITE_BUFFER_SIZE: u32 = 8 * 1024 * 1024; // 8MB
// Connection, read, and write
pub(super) const TCP_CONNECT_TIMEOUT: u64 = 30;
// Keep alive
pub(super) const TCP_KEEP_ALIVE_SECS: u64 = 60;
pub(super) const TCP_KEEP_ALIVE_INTERVAL: u64 = 10;
pub(super) const TCP_KEEP_ALIVE_RETRIES: u32 = 6;

// Maximum number of progress and profile statuses to keep in memory. New statuses evict old ones.
pub(super) const EVENTS_CAPACITY: usize = 8;

// Debugs & ENV Settings
pub const DEBUG_ARROW_ENV_VAR: &str = "CLICKHOUSE_NATIVE_DEBUG_ARROW";
pub(super) const ENABLE_PROFILE_EVENTS: &str = "ENABLE_PROFILE_EVENTS";

#[cfg(test)]
mod tests {
    #[test]
    fn test_version_matches_cargo() {
        let cargo_version = env!("CARGO_PKG_VERSION");
        let parts: Vec<&str> = cargo_version.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "Invalid CARGO_PKG_VERSION format: {cargo_version}, expected X.Y.Z"
        );

        let major = parts[0].parse::<u64>().expect("Invalid major version");
        let minor = parts[1].parse::<u64>().expect("Invalid minor version");
        let patch = parts[2].parse::<u64>().expect("Invalid patch version");

        assert_eq!(
            major,
            super::VERSION_MAJOR,
            "VERSION_MAJOR ({}) does not match Cargo.toml major ({})",
            super::VERSION_MAJOR,
            major
        );
        assert_eq!(
            minor,
            super::VERSION_MINOR,
            "VERSION_MINOR ({}) does not match Cargo.toml minor ({})",
            super::VERSION_MINOR,
            minor
        );
        assert_eq!(
            patch,
            super::VERSION_PATCH,
            "VERSION_PATCH ({}) does not match Cargo.toml patch ({})",
            super::VERSION_PATCH,
            patch
        );
    }
}
