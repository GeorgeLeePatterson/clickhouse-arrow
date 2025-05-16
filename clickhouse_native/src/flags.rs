use std::sync::OnceLock;

use crate::constants::{DEBUG_ARROW_ENV_VAR, ENABLE_PROFILE_EVENTS};

static DEBUG_ARROW_ON: OnceLock<bool> = OnceLock::new();
static PROFILE_EVENTS_ON: OnceLock<bool> = OnceLock::new();

#[allow(dead_code)]
pub(crate) fn debug_arrow() -> bool {
    *DEBUG_ARROW_ON.get_or_init(|| std::env::var(DEBUG_ARROW_ENV_VAR).is_ok())
}

#[allow(dead_code)]
pub(crate) fn enable_profile_events() -> bool {
    *PROFILE_EVENTS_ON.get_or_init(|| std::env::var(ENABLE_PROFILE_EVENTS).is_ok())
}
