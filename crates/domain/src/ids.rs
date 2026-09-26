//! Identifier value objects.

use std::{fmt, sync::Arc};

use uuid::Uuid;

use crate::error::DomainError;

/// Server-assigned order identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderId(Uuid);

impl OrderId {
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        Uuid::try_parse(raw)
            .map(Self)
            .map_err(|_| DomainError::InvalidOrderId)
    }

    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Per-market trade sequence number assigned by the matching engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TradeId(u64);

impl TradeId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TradeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The account that owns an order.
///
/// Backed by `Arc<str>` because the id is copied into every order, trade and
/// event. Cloning an `Arc` only increments a reference count.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AccountId(Arc<str>);

impl AccountId {
    pub const MAX_LEN: usize = 64;

    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        if is_token(raw, Self::MAX_LEN) {
            Ok(Self(Arc::from(raw)))
        } else {
            Err(DomainError::InvalidAccountId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifier chosen by the client so it can recognize its own orders.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientOrderId(Arc<str>);

impl ClientOrderId {
    pub const MAX_LEN: usize = 36;

    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        if is_token(raw, Self::MAX_LEN) {
            Ok(Self(Arc::from(raw)))
        } else {
            Err(DomainError::InvalidClientOrderId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Accepts short ASCII tokens made of letters, digits, `-` and `_`.
///
/// The allow list keeps ids safe to echo in logs, headers and Kafka keys
/// without escaping.
fn is_token(raw: &str, max_len: usize) -> bool {
    !raw.is_empty()
        && raw.len() <= max_len
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_ids_only_accept_safe_tokens() {
        assert!(AccountId::parse("acct_01-A").is_ok());
        assert!(AccountId::parse("").is_err());
        assert!(AccountId::parse("has space").is_err());
        assert!(AccountId::parse("line\nbreak").is_err());
        assert!(AccountId::parse(&"a".repeat(AccountId::MAX_LEN + 1)).is_err());
    }

    #[test]
    fn order_id_round_trips_through_text() {
        let id = OrderId::from_uuid(Uuid::from_u128(7));
        assert_eq!(OrderId::parse(&id.to_string()), Ok(id));
        assert_eq!(OrderId::parse("nope"), Err(DomainError::InvalidOrderId));
    }
}
