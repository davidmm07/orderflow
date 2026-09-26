//! Prints the signature for one request, using the same code as the server.
//!
//! The secret is read from `ORDERFLOW_SECRET` rather than an argument, so it
//! never shows up in the process list or shell history.
//!
//! ```text
//! ORDERFLOW_SECRET=... cargo run -q -p orderflow-api --example sign -- \
//!     TIMESTAMP METHOD PATH_AND_QUERY [BODY]
//! ```

use std::{env, process::ExitCode};

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let (Some(timestamp), Some(method), Some(path)) = (args.first(), args.get(1), args.get(2))
    else {
        eprintln!("usage: sign TIMESTAMP METHOD PATH_AND_QUERY [BODY]");
        return ExitCode::FAILURE;
    };
    let Ok(timestamp) = timestamp.parse::<i64>() else {
        eprintln!("TIMESTAMP must be unix seconds");
        return ExitCode::FAILURE;
    };
    let Ok(secret) = env::var("ORDERFLOW_SECRET") else {
        eprintln!("ORDERFLOW_SECRET is not set");
        return ExitCode::FAILURE;
    };
    let body = args.get(3).map_or("", String::as_str);
    println!(
        "{}",
        orderflow_api::sign_request(secret.as_bytes(), timestamp, method, path, body.as_bytes())
    );
    ExitCode::SUCCESS
}
