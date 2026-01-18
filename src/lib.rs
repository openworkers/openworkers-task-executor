//! OpenWorkers Task Executor
//!
//! A minimal, standalone task executor that uses V8 to run JavaScript/TypeScript tasks.
//! Unlike the full runner, this executor:
//! - Has no database dependency (unless using the `database` feature)
//! - Only implements `fetch` (no KV, Storage, Database bindings)
//! - Executes tasks from files or stdin
//!
//! Perfect for local development, testing, or standalone task execution.
//!
//! ## Features
//!
//! - `v8` (default): Enable V8 runtime for JavaScript execution
//! - `nats`: Enable NATS message queue listener
//! - `database`: Enable PostgreSQL database queue listener with pg_notify

mod ops;

#[cfg(feature = "database")]
pub mod db;

pub use ops::MinimalOps;

pub use openworkers_core::{
    Event, EventType, HttpRequest, HttpResponse, TaskInit, TaskResult, TaskSource,
};
