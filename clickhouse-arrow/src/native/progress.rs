/// Query execution progress.
/// Values are delta and must be summed.
///
/// See <https://clickhouse.com/codebrowser/ClickHouse/src/IO/Progress.h.html>
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub read_rows:           u64,
    pub read_bytes:          u64,
    pub total_rows_to_read:  u64,
    pub total_bytes_to_read: Option<u64>,
    pub written_rows:        Option<u64>,
    pub written_bytes:       Option<u64>,
    pub elapsed_ns:          Option<u64>,
}

impl std::ops::Add for Progress {
    type Output = Progress;

    fn add(self, rhs: Self) -> Self::Output {
        let sum_opt = |opt1, opt2| match (opt1, opt2) {
            (Some(a), Some(b)) => Some(a + b),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        Self::Output {
            read_rows:           self.read_rows + rhs.read_rows,
            read_bytes:          self.read_bytes + rhs.read_bytes,
            total_rows_to_read:  self.total_rows_to_read + rhs.total_rows_to_read,
            total_bytes_to_read: sum_opt(self.total_bytes_to_read, rhs.total_bytes_to_read),
            written_rows:        sum_opt(self.written_rows, rhs.written_rows),
            written_bytes:       sum_opt(self.written_bytes, rhs.written_bytes),
            elapsed_ns:          sum_opt(self.elapsed_ns, rhs.elapsed_ns),
        }
    }
}

impl std::fmt::Display for Progress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Progress | Read | Remaining | W Rows | W Bytes | Elapsed")?;

        let Self {
            read_rows,
            read_bytes,
            total_rows_to_read,
            total_bytes_to_read: _,
            written_rows,
            written_bytes,
            elapsed_ns,
        } = self;

        write!(
            f,
            "{read_rows}/{read_bytes} | {total_rows_to_read} | {written_rows:?} | \
             {written_bytes:?} | {elapsed_ns:?}"
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_default() {
        let progress = Progress::default();
        assert_eq!(progress.read_rows, 0);
        assert_eq!(progress.read_bytes, 0);
        assert_eq!(progress.total_rows_to_read, 0);
        assert_eq!(progress.total_bytes_to_read, None);
        assert_eq!(progress.written_rows, None);
        assert_eq!(progress.written_bytes, None);
        assert_eq!(progress.elapsed_ns, None);
    }

    #[test]
    fn test_progress_creation() {
        let progress = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: Some(10240),
            written_rows:        Some(50),
            written_bytes:       Some(512),
            elapsed_ns:          Some(1_000_000),
        };

        assert_eq!(progress.read_rows, 100);
        assert_eq!(progress.read_bytes, 1024);
        assert_eq!(progress.total_rows_to_read, 1000);
        assert_eq!(progress.total_bytes_to_read, Some(10240));
        assert_eq!(progress.written_rows, Some(50));
        assert_eq!(progress.written_bytes, Some(512));
        assert_eq!(progress.elapsed_ns, Some(1_000_000));
    }

    #[test]
    fn test_progress_clone_copy() {
        let progress = Progress {
            read_rows:           123,
            read_bytes:          456,
            total_rows_to_read:  789,
            total_bytes_to_read: Some(1011),
            written_rows:        Some(121),
            written_bytes:       Some(314),
            elapsed_ns:          Some(1516),
        };

        let cloned = progress;
        let copied = progress;

        assert_eq!(progress, cloned);
        assert_eq!(progress, copied);
    }

    #[test]
    fn test_progress_debug() {
        let progress = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: Some(10240),
            written_rows:        Some(50),
            written_bytes:       Some(512),
            elapsed_ns:          Some(1_000_000),
        };

        let debug_str = format!("{progress:?}");
        assert!(debug_str.contains("Progress"));
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("1024"));
    }

    #[test]
    fn test_progress_add_all_some() {
        let progress1 = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: Some(10240),
            written_rows:        Some(50),
            written_bytes:       Some(512),
            elapsed_ns:          Some(1_000_000),
        };

        let progress2 = Progress {
            read_rows:           200,
            read_bytes:          2048,
            total_rows_to_read:  2000,
            total_bytes_to_read: Some(20480),
            written_rows:        Some(100),
            written_bytes:       Some(1024),
            elapsed_ns:          Some(2_000_000),
        };

        let result = progress1 + progress2;

        assert_eq!(result.read_rows, 300);
        assert_eq!(result.read_bytes, 3072);
        assert_eq!(result.total_rows_to_read, 3000);
        assert_eq!(result.total_bytes_to_read, Some(30720));
        assert_eq!(result.written_rows, Some(150));
        assert_eq!(result.written_bytes, Some(1536));
        assert_eq!(result.elapsed_ns, Some(3_000_000));
    }

    #[test]
    fn test_progress_add_mixed_options() {
        let progress1 = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: Some(10240),
            written_rows:        Some(50),
            written_bytes:       None,
            elapsed_ns:          Some(1_000_000),
        };

        let progress2 = Progress {
            read_rows:           200,
            read_bytes:          2048,
            total_rows_to_read:  2000,
            total_bytes_to_read: None,
            written_rows:        None,
            written_bytes:       Some(1024),
            elapsed_ns:          None,
        };

        let result = progress1 + progress2;

        assert_eq!(result.read_rows, 300);
        assert_eq!(result.read_bytes, 3072);
        assert_eq!(result.total_rows_to_read, 3000);
        assert_eq!(result.total_bytes_to_read, Some(10240)); // Some + None = Some
        assert_eq!(result.written_rows, Some(50)); // Some + None = Some
        assert_eq!(result.written_bytes, Some(1024)); // None + Some = Some
        assert_eq!(result.elapsed_ns, Some(1_000_000)); // Some + None = Some
    }

    #[test]
    fn test_progress_add_all_none() {
        let progress1 = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: None,
            written_rows:        None,
            written_bytes:       None,
            elapsed_ns:          None,
        };

        let progress2 = Progress {
            read_rows:           200,
            read_bytes:          2048,
            total_rows_to_read:  2000,
            total_bytes_to_read: None,
            written_rows:        None,
            written_bytes:       None,
            elapsed_ns:          None,
        };

        let result = progress1 + progress2;

        assert_eq!(result.read_rows, 300);
        assert_eq!(result.read_bytes, 3072);
        assert_eq!(result.total_rows_to_read, 3000);
        assert_eq!(result.total_bytes_to_read, None); // None + None = None
        assert_eq!(result.written_rows, None); // None + None = None
        assert_eq!(result.written_bytes, None); // None + None = None
        assert_eq!(result.elapsed_ns, None); // None + None = None
    }

    #[test]
    fn test_progress_display() {
        let progress = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: Some(10240),
            written_rows:        Some(50),
            written_bytes:       Some(512),
            elapsed_ns:          Some(1_000_000),
        };

        let display_str = format!("{progress}");

        // Check that the display string contains expected components
        assert!(display_str.contains("Progress"));
        assert!(display_str.contains("Read"));
        assert!(display_str.contains("Remaining"));
        assert!(display_str.contains("W Rows"));
        assert!(display_str.contains("W Bytes"));
        assert!(display_str.contains("Elapsed"));
        assert!(display_str.contains("100/1024"));
        assert!(display_str.contains("1000"));
        assert!(display_str.contains("Some(50)"));
        assert!(display_str.contains("Some(512)"));
        assert!(display_str.contains("Some(1000000)"));
    }

    #[test]
    fn test_progress_display_with_nones() {
        let progress = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: None,
            written_rows:        None,
            written_bytes:       None,
            elapsed_ns:          None,
        };

        let display_str = format!("{progress}");

        // Check that None values are displayed as "None"
        assert!(display_str.contains("None"));
        assert!(display_str.contains("100/1024"));
        assert!(display_str.contains("1000"));
    }

    #[test]
    fn test_progress_equality() {
        let progress1 = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: Some(10240),
            written_rows:        Some(50),
            written_bytes:       Some(512),
            elapsed_ns:          Some(1_000_000),
        };

        let progress2 = Progress {
            read_rows:           100,
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: Some(10240),
            written_rows:        Some(50),
            written_bytes:       Some(512),
            elapsed_ns:          Some(1_000_000),
        };

        let progress3 = Progress {
            read_rows:           200, // Different value
            read_bytes:          1024,
            total_rows_to_read:  1000,
            total_bytes_to_read: Some(10240),
            written_rows:        Some(50),
            written_bytes:       Some(512),
            elapsed_ns:          Some(1_000_000),
        };

        assert_eq!(progress1, progress2);
        assert_ne!(progress1, progress3);
    }
}
