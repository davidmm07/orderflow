//! Wall clock adapter.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use orderflow_application::Clock;
use orderflow_domain::Timestamp;

/// System time that never moves backwards.
///
/// NTP corrections can step the wall clock back. Event timestamps must not
/// go back within a stream, so this clock returns the maximum of the
/// current reading and the last value it handed out.
#[derive(Debug, Default)]
pub struct MonotonicClock {
    last: AtomicU64,
}

impl Clock for MonotonicClock {
    fn now(&self) -> Timestamp {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
            });
        let previous = self.last.fetch_max(wall, Ordering::AcqRel);
        Timestamp::from_unix_nanos(previous.max(wall))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readings_never_decrease() {
        let clock = MonotonicClock::default();
        clock.last.store(u64::MAX - 1, Ordering::Release);
        let first = clock.now();
        let second = clock.now();
        assert!(second >= first);
        assert_eq!(first.unix_nanos(), u64::MAX - 1);
    }
}
