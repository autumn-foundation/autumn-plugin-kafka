//! Helpers for tests that need a real broker.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the broker list from `KAFKA_BROKERS`.
///
/// Returns `None` and prints a note if the variable is not set.
pub fn brokers(test: &str) -> Option<String> {
    let brokers = std::env::var("KAFKA_BROKERS")
        .ok()
        .filter(|b| !b.is_empty());
    if brokers.is_none() {
        eprintln!("skip {test}: set KAFKA_BROKERS to run it");
    }
    brokers
}

/// Returns a name that no other test run uses.
pub fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{n}")
}
