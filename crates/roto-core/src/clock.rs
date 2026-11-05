//! Process-wide virtual clock. Real time plus an offset that tests can move forward, so timers
//! (Luau service jobs, `os.time()` in service scripts) fire without waiting.
//!
//! Only code that calls [`now_ms`] or [`now_secs`] sees the offset; services that read
//! `SystemTime` directly keep real time.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static OFFSET_MS: AtomicI64 = AtomicI64::new(0);

fn real_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Milliseconds since the Unix epoch on the virtual clock.
pub fn now_ms() -> i64 {
    real_ms().saturating_add(OFFSET_MS.load(Ordering::SeqCst))
}

/// Seconds since the Unix epoch on the virtual clock.
pub fn now_secs() -> i64 {
    now_ms().div_euclid(1000)
}

/// How far the virtual clock is ahead of real time.
pub fn offset_ms() -> i64 {
    OFFSET_MS.load(Ordering::SeqCst)
}

/// Moves the clock forward. Time never goes backwards, so a negative step is rejected.
/// Returns the new offset.
pub fn advance_ms(ms: i64) -> Result<i64, &'static str> {
    if ms < 0 {
        return Err("the clock can only move forward");
    }
    Ok(OFFSET_MS.fetch_add(ms, Ordering::SeqCst).saturating_add(ms))
}

/// Moves the clock forward to `ms` since the epoch; does nothing if that time has passed.
pub fn advance_to_ms(ms: i64) {
    let now = now_ms();
    if ms > now {
        OFFSET_MS.fetch_add(ms - now, Ordering::SeqCst);
    }
}

/// Returns the clock to real time.
pub fn reset() {
    OFFSET_MS.store(0, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_moves_now_forward_and_rejects_negative_steps() {
        let before = now_ms();
        advance_ms(60_000).unwrap();
        assert!(now_ms() - before >= 60_000);
        assert!(advance_ms(-1).is_err());
        reset();
        assert_eq!(offset_ms(), 0);
    }
}
