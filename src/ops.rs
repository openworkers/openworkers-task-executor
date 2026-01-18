//! Minimal operations handler - fetch only
//!
//! Implements `OperationsHandler` with only `handle_fetch` and `handle_log`.
//! All other operations return errors.

use bytes::Bytes;
use openworkers_core::{
    HttpMethod, HttpRequest, HttpResponse, LogLevel, OpFuture, OperationsHandler, RequestBody,
    ResponseBody,
};
use reqwest::Client;
use std::sync::OnceLock;

/// Thread-local HTTP client for connection reuse
fn http_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .pool_max_idle_per_host(10)
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client")
    })
}

/// Minimal operations handler that only supports fetch and logging.
///
/// All binding operations (KV, Storage, Database, Worker) return errors.
pub struct MinimalOps {
    /// Whether to print logs to stderr (default: true)
    pub print_logs: bool,
}

impl MinimalOps {
    pub fn new() -> Self {
        Self { print_logs: true }
    }

    pub fn silent() -> Self {
        Self { print_logs: false }
    }
}

impl Default for MinimalOps {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationsHandler for MinimalOps {
    fn handle_fetch(&self, request: HttpRequest) -> OpFuture<'_, Result<HttpResponse, String>> {
        Box::pin(async move {
            let client = http_client();

            let method = match request.method {
                HttpMethod::Get => reqwest::Method::GET,
                HttpMethod::Post => reqwest::Method::POST,
                HttpMethod::Put => reqwest::Method::PUT,
                HttpMethod::Delete => reqwest::Method::DELETE,
                HttpMethod::Head => reqwest::Method::HEAD,
                HttpMethod::Options => reqwest::Method::OPTIONS,
                HttpMethod::Patch => reqwest::Method::PATCH,
            };

            let mut req_builder = client.request(method, &request.url);

            for (key, value) in &request.headers {
                req_builder = req_builder.header(key, value);
            }

            if let RequestBody::Bytes(body) = request.body {
                req_builder = req_builder.body(body);
            }

            let response = req_builder
                .send()
                .await
                .map_err(|e| format!("Fetch error: {}", e))?;

            let status = response.status().as_u16();
            let headers: Vec<(String, String)> = response
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();

            let body = response
                .bytes()
                .await
                .map_err(|e| format!("Failed to read response body: {}", e))?;

            Ok(HttpResponse {
                status,
                headers,
                body: ResponseBody::Bytes(Bytes::from(body)),
            })
        })
    }

    fn handle_log(&self, level: LogLevel, message: String) {
        if self.print_logs {
            let level_str = match level {
                LogLevel::Debug => "DEBUG",
                LogLevel::Info => "INFO",
                LogLevel::Warn => "WARN",
                LogLevel::Error => "ERROR",
                LogLevel::Log => "LOG",
                LogLevel::Trace => "TRACE",
            };
            eprintln!("[{}] {}", level_str, message);
        }
    }
}
