//! HMAC request signing, in the style used by exchange REST APIs.
//!
//! A client signs `timestamp + METHOD + path_and_query + body` with its
//! shared secret using HMAC-SHA256 and sends the hex digest with its key id
//! and the timestamp. The server recomputes the digest and compares it in
//! constant time. Because the body is part of the signature, a proxy or an
//! attacker cannot alter an order in transit, and because the timestamp is,
//! a captured request stops being replayable after the tolerance window.

use std::{
    collections::HashMap,
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, Method},
    middleware::Next,
    response::Response,
};
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::LengthLimitError;
use orderflow_domain::AccountId;
use sha2::Sha256;

use crate::{error::ApiError, state::AppState};

pub const API_KEY_HEADER: &str = "x-orderflow-key";
pub const TIMESTAMP_HEADER: &str = "x-orderflow-timestamp";
pub const SIGNATURE_HEADER: &str = "x-orderflow-signature";

/// Shortest accepted secret. 32 bytes matches the HMAC-SHA256 block
/// security level; anything shorter is brute-forceable offline from a
/// single captured request.
pub const MIN_SECRET_LEN: usize = 32;

type HmacSha256 = Hmac<Sha256>;

/// Used when the key id is unknown, so that path does the same amount of
/// work as a real verification and response timing does not reveal which
/// key ids exist.
const DECOY_SECRET: &[u8; MIN_SECRET_LEN] = b"orderflow-decoy-secret-not-valid";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CredentialError {
    #[error("API secrets must be at least {MIN_SECRET_LEN} bytes long")]
    SecretTooShort,
}

/// A shared secret bound to one account.
#[derive(Clone)]
pub struct Credential {
    account: AccountId,
    secret: Box<[u8]>,
}

impl Credential {
    pub fn new(account: AccountId, secret: impl Into<Vec<u8>>) -> Result<Self, CredentialError> {
        let secret = secret.into();
        if secret.len() < MIN_SECRET_LEN {
            return Err(CredentialError::SecretTooShort);
        }
        Ok(Self {
            account,
            secret: secret.into_boxed_slice(),
        })
    }
}

// Written by hand so a secret can never reach a log line through `{:?}`.
impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("account", &self.account)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Why a signature was rejected. Logged at debug level, never returned.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("missing or non-ASCII header {0}")]
    MissingHeader(&'static str),
    #[error("unknown API key")]
    UnknownKey,
    #[error("timestamp is not a unix time in seconds")]
    MalformedTimestamp,
    #[error("timestamp is outside the accepted window")]
    StaleTimestamp,
    #[error("signature is not hex encoded")]
    MalformedSignature,
    #[error("signature does not match")]
    BadSignature,
}

/// Verifies signed requests against a set of credentials.
#[derive(Debug)]
pub struct Authenticator {
    credentials: HashMap<String, Credential>,
    tolerance: Duration,
    max_body_bytes: usize,
}

impl Authenticator {
    /// `tolerance` bounds clock skew and the replay window; `max_body_bytes`
    /// is the most this verifier will buffer to hash a body.
    pub fn new(
        credentials: impl IntoIterator<Item = (String, Credential)>,
        tolerance: Duration,
        max_body_bytes: usize,
    ) -> Self {
        Self {
            credentials: credentials.into_iter().collect(),
            tolerance,
            max_body_bytes,
        }
    }

    pub fn verify(
        &self,
        method: &Method,
        path_and_query: &str,
        headers: &HeaderMap,
        body: &[u8],
        now_unix: i64,
    ) -> Result<AccountId, AuthError> {
        let key_id = header(headers, API_KEY_HEADER)?;
        let timestamp = header(headers, TIMESTAMP_HEADER)?;
        let signature = header(headers, SIGNATURE_HEADER)?;

        let issued_at: i64 = timestamp
            .parse()
            .map_err(|_| AuthError::MalformedTimestamp)?;
        if now_unix.abs_diff(issued_at) > self.tolerance.as_secs() {
            return Err(AuthError::StaleTimestamp);
        }
        let signature = hex::decode(signature).map_err(|_| AuthError::MalformedSignature)?;

        let credential = self.credentials.get(key_id);
        let secret = credential.map_or(&DECOY_SECRET[..], |c| &c.secret);
        let verified = mac(secret, timestamp, method.as_str(), path_and_query, body)
            .is_some_and(|mac| mac.verify_slice(&signature).is_ok());

        match credential {
            None => Err(AuthError::UnknownKey),
            Some(_) if !verified => Err(AuthError::BadSignature),
            Some(credential) => Ok(credential.account.clone()),
        }
    }
}

/// Computes the signature a client must send. Public so SDKs, tests and
/// the example scripts share one definition of the signing string.
pub fn sign_request(
    secret: &[u8],
    timestamp: i64,
    method: &str,
    path_and_query: &str,
    body: &[u8],
) -> String {
    mac(secret, &timestamp.to_string(), method, path_and_query, body)
        .map(|mac| hex::encode(mac.finalize().into_bytes()))
        .unwrap_or_default()
}

/// HMAC accepts keys of any length, so `None` does not occur in practice.
/// Returning an `Option` covers it without an `expect`.
fn mac(
    secret: &[u8],
    timestamp: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> Option<HmacSha256> {
    let mut mac = HmacSha256::new_from_slice(secret).ok()?;
    mac.update(timestamp.as_bytes());
    mac.update(method.as_bytes());
    mac.update(path.as_bytes());
    mac.update(body);
    Some(mac)
}

fn header<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<&'a str, AuthError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or(AuthError::MissingHeader(name))
}

/// The account a request was authenticated as, stored in request extensions.
#[derive(Debug, Clone)]
pub struct Authenticated(pub AccountId);

/// Middleware that rejects any request without a valid signature.
///
/// The body has to be buffered to be hashed. The buffer is capped at the
/// configured limit, so an unauthenticated client cannot make the server
/// hold an arbitrarily large body in memory.
pub(crate) async fn require_signature(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, state.authenticator.max_body_bytes)
        .await
        .map_err(|error| {
            let too_large = std::error::Error::source(&error)
                .is_some_and(|source| source.is::<LengthLimitError>());
            if too_large {
                ApiError::payload_too_large()
            } else {
                ApiError::new(
                    axum::http::StatusCode::BAD_REQUEST,
                    "unreadable_body",
                    "Request body could not be read",
                    "The request body ended unexpectedly.",
                )
            }
        })?;

    let path = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path(), |pq| pq.as_str());
    let account = state
        .authenticator
        .verify(&parts.method, path, &parts.headers, &bytes, unix_now())
        .map_err(|reason| {
            tracing::debug!(%reason, "rejected request signature");
            ApiError::unauthenticated()
        })?;

    let mut request = Request::from_parts(parts, Body::from(bytes));
    request.extensions_mut().insert(Authenticated(account));
    Ok(next.run(request).await)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn authenticator() -> Authenticator {
        let credential = Credential::new(AccountId::parse("alice").unwrap(), SECRET).unwrap();
        Authenticator::new(
            [("key-1".to_owned(), credential)],
            Duration::from_secs(30),
            1024,
        )
    }

    fn headers(key: &str, timestamp: i64, signature: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(API_KEY_HEADER, HeaderValue::from_str(key).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from(timestamp));
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(signature).unwrap());
        headers
    }

    #[test]
    fn accepts_a_correct_signature() {
        let signature = sign_request(SECRET, 1_000, "POST", "/v1/x", b"{}");
        let result = authenticator().verify(
            &Method::POST,
            "/v1/x",
            &headers("key-1", 1_000, &signature),
            b"{}",
            1_010,
        );
        assert_eq!(result, Ok(AccountId::parse("alice").unwrap()));
    }

    #[test]
    fn rejects_tampered_bodies_paths_and_methods() {
        let auth = authenticator();
        let signature = sign_request(SECRET, 1_000, "POST", "/v1/x", b"{\"q\":1}");
        let h = headers("key-1", 1_000, &signature);
        let bad = Err(AuthError::BadSignature);
        assert_eq!(
            auth.verify(&Method::POST, "/v1/x", &h, b"{\"q\":9}", 1_000),
            bad
        );
        assert_eq!(
            auth.verify(&Method::POST, "/v1/y", &h, b"{\"q\":1}", 1_000),
            bad
        );
        assert_eq!(
            auth.verify(&Method::DELETE, "/v1/x", &h, b"{\"q\":1}", 1_000),
            bad
        );
    }

    #[test]
    fn rejects_requests_outside_the_time_window() {
        let signature = sign_request(SECRET, 1_000, "GET", "/", b"");
        let result = authenticator().verify(
            &Method::GET,
            "/",
            &headers("key-1", 1_000, &signature),
            b"",
            1_031,
        );
        assert_eq!(result, Err(AuthError::StaleTimestamp));
    }

    #[test]
    fn rejects_unknown_keys_and_missing_headers() {
        let auth = authenticator();
        let signature = sign_request(SECRET, 1_000, "GET", "/", b"");
        assert_eq!(
            auth.verify(
                &Method::GET,
                "/",
                &headers("key-2", 1_000, &signature),
                b"",
                1_000
            ),
            Err(AuthError::UnknownKey)
        );
        assert_eq!(
            auth.verify(&Method::GET, "/", &HeaderMap::new(), b"", 1_000),
            Err(AuthError::MissingHeader(API_KEY_HEADER))
        );
    }

    #[test]
    fn short_secrets_are_refused_and_never_printed() {
        let account = AccountId::parse("alice").unwrap();
        assert_eq!(
            Credential::new(account.clone(), b"short".to_vec()).unwrap_err(),
            CredentialError::SecretTooShort
        );
        let printed = format!("{:?}", Credential::new(account, SECRET).unwrap());
        assert!(!printed.contains("0123456789"));
    }
}
