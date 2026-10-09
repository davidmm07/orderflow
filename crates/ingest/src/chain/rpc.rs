//! JSON-RPC 2.0 over HTTP, the protocol of both Ethereum and Solana nodes.

use async_trait::async_trait;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};

use crate::{error::SourceError, http::HttpClient};

/// Sends one JSON-RPC call and returns its `result`.
///
/// Indexers depend on this trait rather than on HTTP, so tests replay
/// recorded node responses and simulate reorgs without a network.
#[async_trait]
pub trait RpcTransport: Send + Sync + 'static {
    async fn call(&self, method: &str, params: Value) -> Result<Value, SourceError>;
}

/// Calls `method` and deserializes its result.
pub(crate) async fn call<T: DeserializeOwned>(
    rpc: &dyn RpcTransport,
    method: &str,
    params: Value,
) -> Result<T, SourceError> {
    let result = rpc.call(method, params).await?;
    serde_json::from_value(result).map_err(SourceError::decode)
}

#[derive(Debug, Deserialize)]
struct Response {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcErrorBody>,
}

#[derive(Debug, Deserialize)]
struct RpcErrorBody {
    code: i64,
    #[serde(default)]
    message: String,
}

/// JSON-RPC client for a node URL such as a public endpoint or a provider
/// URL with an API key in its path. The URL is a secret: it is never
/// logged, and `Debug` does not print it.
pub struct HttpRpc {
    http: HttpClient,
    url: String,
}

impl HttpRpc {
    pub fn new(http: HttpClient, url: impl Into<String>) -> Self {
        Self {
            http,
            url: url.into(),
        }
    }
}

impl std::fmt::Debug for HttpRpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpRpc(<url redacted>)")
    }
}

#[async_trait]
impl RpcTransport for HttpRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, SourceError> {
        let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response: Response = self.http.post_json(&self.url, &request).await?;
        match (response.result, response.error) {
            (_, Some(error)) => Err(SourceError::rpc(error.code, &error.message)),
            (Some(result), None) => Ok(result),
            (None, None) => Err(SourceError::decode("response has neither result nor error")),
        }
    }
}
