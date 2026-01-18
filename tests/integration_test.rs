//! Integration tests for the task executor
//!
//! These tests require the V8 runtime feature.

#![cfg(feature = "v8")]

use openworkers_core::{Event, RuntimeLimits, Script, TaskSource, WorkerCode};
use openworkers_task_executor::MinimalOps;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::LocalSet;

fn fixtures_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

async fn run_task_file(
    filename: &str,
    payload: Option<serde_json::Value>,
) -> openworkers_core::TaskResult {
    let script_path = fixtures_path().join(filename);
    let script_content = std::fs::read_to_string(&script_path)
        .unwrap_or_else(|_| panic!("Failed to read {}", filename));

    let script = Script {
        code: WorkerCode::JavaScript(script_content),
        env: None,
        bindings: vec![],
    };

    let ops: openworkers_core::OperationsHandle = Arc::new(MinimalOps::silent());

    let limits = RuntimeLimits {
        max_wall_clock_time_ms: 5000,
        max_cpu_time_ms: 5000,
        ..Default::default()
    };

    let task_id = format!("test-{}", uuid::Uuid::new_v4());
    let source = TaskSource::Invoke {
        origin: Some("test".to_string()),
    };

    let (event, rx) = Event::task(task_id, payload, Some(source), 1);

    let local = LocalSet::new();

    local
        .run_until(async move {
            use openworkers_runtime_v8::Worker;

            let mut worker = Worker::new_with_ops(script, Some(limits), ops)
                .await
                .expect("Failed to create worker");

            worker.exec(event).await.expect("Failed to execute task");

            rx.await.expect("Failed to receive result")
        })
        .await
}

#[tokio::test]
async fn test_basic_task() {
    let payload = serde_json::json!({ "name": "test", "value": 42 });
    let result = run_task_file("basic_task.js", Some(payload.clone())).await;

    assert!(result.success, "Task should succeed");

    let data = result.data.expect("Should have data");
    assert_eq!(data["received"], payload);
}

#[tokio::test]
async fn test_basic_task_no_payload() {
    let result = run_task_file("basic_task.js", None).await;

    assert!(result.success, "Task should succeed");

    let data = result.data.expect("Should have data");
    assert!(data["received"].is_null(), "Payload should be null");
}

#[tokio::test]
async fn test_error_task() {
    let result = run_task_file("error_task.js", None).await;

    assert!(!result.success, "Task should fail");
    assert!(
        result
            .error
            .as_ref()
            .unwrap()
            .contains("Intentional test error"),
        "Error message should contain 'Intentional test error', got: {:?}",
        result.error
    );
}

#[tokio::test]
async fn test_es_modules_task() {
    let result = run_task_file("es_modules_task.js", None).await;

    assert!(result.success, "Task should succeed");

    let data = result.data.expect("Should have data");
    assert_eq!(data["style"], "es-modules");
}

#[tokio::test]
async fn test_return_task() {
    let payload = serde_json::json!({ "test": true });
    let result = run_task_file("return_task.js", Some(payload.clone())).await;

    assert!(result.success, "Task should succeed");

    let data = result.data.expect("Should have data");
    assert_eq!(data["method"], "return");
    assert_eq!(data["payload"], payload);
}

#[tokio::test]
async fn test_async_task() {
    let result = run_task_file("async_task.js", None).await;

    assert!(result.success, "Task should succeed");

    let data = result.data.expect("Should have data");
    assert_eq!(data["async"], true);
}

#[tokio::test]
async fn test_wait_until_task() {
    let result = run_task_file("wait_until_task.js", None).await;

    assert!(result.success, "Task should succeed");

    let data = result.data.expect("Should have data");
    assert_eq!(data["immediate"], true);
    // Note: waitUntil should complete before the worker returns
}
