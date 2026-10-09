//! Failures of a market data source.

use std::{fmt::Display, time::Duration};

use orderflow_marketdata::ports::StoreError;

/// Longest upstream text kept in an error. Upstream bodies are untrusted
/// and can be large; logs get a bounded excerpt, never the whole thing.
const MAX_DETAIL_CHARS: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceError {
    /// Connect failure, timeout or reset. Never contains the URL, which
    /// may carry an API key.
    #[error("transport error: {0}")]
    Transport(String),
    #[error("upstream answered HTTP {status}")]
    Status {
        status: u16,
        /// From a `Retry-After` header, when the upstream sent one.
        retry_after: Option<Duration>,
    },
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    /// The upstream answered, but not with what its API promises.
    #[error("unexpected response: {0}")]
    Decode(String),
    /// The upstream reported an error in its own protocol.
    #[error("upstream error: {0}")]
    Upstream(String),
    #[error("chain data inconsistent: {0}")]
    Chain(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the aggregator stopped accepting observations")]
    Closed,
}

impl SourceError {
    pub fn decode(detail: impl Display) -> Self {
        Self::Decode(excerpt(&detail.to_string()))
    }

    pub fn upstream(detail: impl Display) -> Self {
        Self::Upstream(excerpt(&detail.to_string()))
    }

    pub fn rpc(code: i64, message: &str) -> Self {
        Self::Rpc {
            code,
            message: excerpt(message),
        }
    }

    /// Short label for metrics.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Transport(_) => "transport",
            Self::Status { .. } => "http_status",
            Self::Rpc { .. } => "rpc",
            Self::Decode(_) => "decode",
            Self::Upstream(_) => "upstream",
            Self::Chain(_) => "chain",
            Self::Store(_) => "store",
            Self::Closed => "closed",
        }
    }

    pub const fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Status { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// First `MAX_DETAIL_CHARS` characters of `text`, with control characters
/// replaced so an upstream cannot forge log lines.
pub(crate) fn excerpt(text: &str) -> String {
    text.chars()
        .take(MAX_DETAIL_CHARS)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_text_is_bounded_and_single_line() {
        let error = SourceError::decode(format!("bad\nline{}", "x".repeat(1_000)));
        let SourceError::Decode(text) = error else {
            unreachable!()
        };
        assert_eq!(text.chars().count(), MAX_DETAIL_CHARS);
        assert!(!text.contains('\n'));
    }
}
