pub mod arrow_helpers;
pub mod constants;
pub mod native_helpers;

pub const SEP: &str = "\n-------------------------------\n";

/// Little helper function to print headers for tests
pub fn header(qid: impl std::fmt::Display, msg: impl AsRef<str>) {
    eprintln!("{SEP} Query ID = {qid}\n {} {SEP}", msg.as_ref());
}
