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
