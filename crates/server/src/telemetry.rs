//! Structured logging setup.

use tracing_subscriber::EnvFilter;

use crate::config::LogFormat;

/// Installs the global subscriber. `RUST_LOG` overrides the default filter.
///
/// JSON is the production format: one object per line, ready for a log
/// pipeline, with the current span (method, path, request id) attached.
pub fn init(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    let result = match format {
        LogFormat::Json => builder
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .try_init(),
        LogFormat::Pretty => builder.try_init(),
    };
    if let Err(error) = result {
        eprintln!("logging was already initialized: {error}");
    }
}
