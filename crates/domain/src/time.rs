//! Time as seen by the domain.

/// Nanoseconds since the Unix epoch.
///
/// The domain never reads a clock. Callers pass the current time in, so the
/// same sequence of commands and timestamps always produces the same events,
/// which is what makes the engine replayable from a journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const fn from_unix_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    pub const fn unix_nanos(self) -> u64 {
        self.0
    }
}
