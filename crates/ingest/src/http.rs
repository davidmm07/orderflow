//! HTTP client shared by REST connectors and JSON-RPC indexers.

use std::{error::Error as _, time::Duration};

use reqwest::{Response, header::RETRY_AFTER};
use serde::{Serialize, de::DeserializeOwned};

use crate::error::{SourceError, excerpt};

/// Largest response body accepted. A misbehaving upstream cannot make the
/// process buffer an unbounded body.
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// A pooled HTTPS client with timeouts, a size limit and error messages
/// that never contain the request URL.
///
/// RPC URLs often embed an API key (`https://host/v2/<key>`), and reqwest
/// errors print the URL by default. Every error built here strips it.
#[derive(Debug, Clone)]
pub struct HttpClient {
    client: reqwest::Client,
}

impl HttpClient {
    pub fn new(timeout: Duration) -> Result<Self, SourceError> {
        install_crypto_provider();
        let client = reqwest::Client::builder()
            .user_agent(concat!("orderflow-pricefeed/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(5))
            .timeout(timeout)
            .build()
            .map_err(transport)?;
        Ok(Self { client })
    }

    pub async fn get_json<T: DeserializeOwned>(
        &self,
        url: &str,
        headers: &[(&'static str, &str)],
    ) -> Result<T, SourceError> {
        let mut request = self.client.get(url);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        decode(request.send().await.map_err(transport)?).await
    }

    pub async fn post_json<B: Serialize + Sync, T: DeserializeOwned>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, SourceError> {
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(transport)?;
        decode(response).await
    }
}

/// Installs rustls' ring provider as the process default. Every TLS client
/// in the service (HTTP, WebSocket, Postgres, Redis) then uses the same
/// pure Rust crypto. Installing twice is harmless.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

async fn decode<T: DeserializeOwned>(mut response: Response) -> Result<T, SourceError> {
    let status = response.status();
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        return Err(SourceError::Status {
            status: status.as_u16(),
            retry_after,
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BODY_BYTES as u64)
    {
        return Err(SourceError::decode("response body too large"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(transport)? {
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(SourceError::decode("response body too large"));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(SourceError::decode)
}

/// Describes a reqwest error with its causes, but without the URL.
fn transport(error: reqwest::Error) -> SourceError {
    let error = error.without_url();
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(inner) = cause {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        cause = inner.source();
    }
    SourceError::Transport(excerpt(&text))
}
