use std::sync::OnceLock;

use crate::constants::*;

static DEBUG_ARROW_ON: OnceLock<bool> = OnceLock::new();

#[allow(dead_code)]
pub(crate) fn debug_arrow() -> bool {
    *DEBUG_ARROW_ON.get_or_init(|| {
        std::env::var(DEBUG_ARROW_ENV_VAR).ok().is_some_and(|v| v.eq_ignore_ascii_case("true"))
    })
}

pub(crate) fn conn_read_buffer_size() -> usize {
    std::env::var(CONN_READ_BUFFER_ENV_VAR)
        .ok()
        .and_then(|e| e.parse::<usize>().ok())
        .unwrap_or(CONN_READ_BUFFER_DEFAULT)
}

pub(crate) fn conn_write_buffer_size() -> usize {
    std::env::var(CONN_WRITE_BUFFER_ENV_VAR)
        .ok()
        .and_then(|e| e.parse::<usize>().ok())
        .unwrap_or(CONN_WRITE_BUFFER_DEFAULT)
}
