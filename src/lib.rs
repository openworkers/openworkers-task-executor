//! OpenWorkers Task Executor
//!
//! A minimal, standalone task executor that uses V8 to run JavaScript/TypeScript tasks.
//! Unlike the full runner, this executor:
//! - Has no database dependency
//! - Only implements `fetch` (no KV, Storage, Database bindings)
//! - Executes tasks from files or stdin
//!
//! Perfect for local development, testing, or standalone task execution.

mod ops;

pub use ops::MinimalOps;

pub use openworkers_core::{
    Event, EventType, HttpRequest, HttpResponse, TaskInit, TaskResult, TaskSource,
};
