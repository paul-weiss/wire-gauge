//! Scheduling support for the load generator.

use std::thread;
use std::time::{Duration, Instant};

/// How far ahead of the target we stop sleeping and start spinning. OS sleep
/// wakes late by tens to hundreds of microseconds; the spin tail eats that
/// error so sends land on schedule at the cost of one busy core.
const SPIN_WINDOW: Duration = Duration::from_micros(300);

/// Sleep-then-spin until `target`. Returns immediately if it has passed.
pub fn spin_sleep_until(target: Instant) {
    loop {
        let now = Instant::now();
        if now >= target {
            return;
        }
        let remaining = target - now;
        if remaining > SPIN_WINDOW {
            thread::sleep(remaining - SPIN_WINDOW);
        } else {
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wakes_on_time_not_early() {
        let start = Instant::now();
        spin_sleep_until(start + Duration::from_millis(5));
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(5),
            "woke early: {elapsed:?}"
        );
        // Generous upper bound: this asserts "not wildly late", not precision.
        assert!(
            elapsed < Duration::from_millis(50),
            "woke very late: {elapsed:?}"
        );
    }

    #[test]
    fn past_target_returns_immediately() {
        let start = Instant::now();
        spin_sleep_until(start - Duration::from_millis(1));
        assert!(start.elapsed() < Duration::from_millis(5));
    }
}
