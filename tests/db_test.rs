//! Database integration tests
//!
//! These tests require a PostgreSQL database. Set DATABASE_URL to run them:
//!
//! ```bash
//! DATABASE_URL=postgres://user:pass@localhost/test cargo test --features v8,database db_
//! ```
//!
//! The tests will create and clean up their own tables.

#![cfg(all(feature = "database", feature = "v8"))]

use openworkers_task_executor::db::{DbPool, TaskCompletion};
use sqlx::Row;
use uuid::Uuid;

/// Get database URL from environment, skip test if not set
fn get_database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok()
}

/// Create test table with unique name
async fn setup_test_table(pool: &sqlx::PgPool, table_name: &str) {
    let query = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {table} (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            script TEXT NOT NULL,
            payload JSONB,
            status TEXT NOT NULL DEFAULT 'pending',
            result JSONB,
            error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ
        )
        "#,
        table = table_name
    );

    sqlx::query(&query).execute(pool).await.unwrap();
}

/// Clean up test table
async fn cleanup_test_table(pool: &sqlx::PgPool, table_name: &str) {
    let query = format!("DROP TABLE IF EXISTS {}", table_name);
    sqlx::query(&query).execute(pool).await.unwrap();
}

/// Insert a test task
async fn insert_task(pool: &sqlx::PgPool, table_name: &str, script: &str) -> Uuid {
    let query = format!(
        "INSERT INTO {} (script) VALUES ($1) RETURNING id",
        table_name
    );

    let row = sqlx::query(&query)
        .bind(script)
        .fetch_one(pool)
        .await
        .unwrap();

    row.get("id")
}

/// Get task status from database
async fn get_task_status(pool: &sqlx::PgPool, table_name: &str, task_id: Uuid) -> String {
    let query = format!("SELECT status FROM {} WHERE id = $1", table_name);

    let row = sqlx::query(&query)
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap();

    row.get("status")
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_db_claim_pending_task() {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    };

    let table_name = format!("test_tasks_{}", Uuid::new_v4().simple());
    let db_pool = DbPool::connect(&database_url, &table_name).await.unwrap();

    // Setup
    setup_test_table(db_pool.pool(), &table_name).await;

    // Insert a task
    let task_id = insert_task(db_pool.pool(), &table_name, "worker.js").await;

    // Claim the task
    let task = db_pool.claim_pending_task().await.unwrap();
    assert!(task.is_some());

    let task = task.unwrap();
    assert_eq!(task.id, task_id);
    assert_eq!(task.script, "worker.js");

    // Verify status changed to running
    let status = get_task_status(db_pool.pool(), &table_name, task_id).await;
    assert_eq!(status, "running");

    // Cleanup
    cleanup_test_table(db_pool.pool(), &table_name).await;
}

#[tokio::test]
async fn test_db_claim_no_pending_tasks() {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    };

    let table_name = format!("test_tasks_{}", Uuid::new_v4().simple());
    let db_pool = DbPool::connect(&database_url, &table_name).await.unwrap();

    // Setup (empty table)
    setup_test_table(db_pool.pool(), &table_name).await;

    // Try to claim - should return None
    let task = db_pool.claim_pending_task().await.unwrap();
    assert!(task.is_none());

    // Cleanup
    cleanup_test_table(db_pool.pool(), &table_name).await;
}

#[tokio::test]
async fn test_db_complete_task_success() {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    };

    let table_name = format!("test_tasks_{}", Uuid::new_v4().simple());
    let db_pool = DbPool::connect(&database_url, &table_name).await.unwrap();

    // Setup
    setup_test_table(db_pool.pool(), &table_name).await;

    // Insert and claim a task
    let task_id = insert_task(db_pool.pool(), &table_name, "worker.js").await;
    let _ = db_pool.claim_pending_task().await.unwrap();

    // Complete the task successfully
    let completion = TaskCompletion {
        result: Some(serde_json::json!({"output": "success"})),
        error: None,
    };

    db_pool.complete_task(task_id, completion).await.unwrap();

    // Verify status
    let status = get_task_status(db_pool.pool(), &table_name, task_id).await;
    assert_eq!(status, "completed");

    // Cleanup
    cleanup_test_table(db_pool.pool(), &table_name).await;
}

#[tokio::test]
async fn test_db_complete_task_failure() {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    };

    let table_name = format!("test_tasks_{}", Uuid::new_v4().simple());
    let db_pool = DbPool::connect(&database_url, &table_name).await.unwrap();

    // Setup
    setup_test_table(db_pool.pool(), &table_name).await;

    // Insert and claim a task
    let task_id = insert_task(db_pool.pool(), &table_name, "worker.js").await;
    let _ = db_pool.claim_pending_task().await.unwrap();

    // Complete the task with error
    let completion = TaskCompletion {
        result: None,
        error: Some("Script not found".to_string()),
    };

    db_pool.complete_task(task_id, completion).await.unwrap();

    // Verify status
    let status = get_task_status(db_pool.pool(), &table_name, task_id).await;
    assert_eq!(status, "failed");

    // Cleanup
    cleanup_test_table(db_pool.pool(), &table_name).await;
}

#[tokio::test]
async fn test_db_fifo_order() {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    };

    let table_name = format!("test_tasks_{}", Uuid::new_v4().simple());
    let db_pool = DbPool::connect(&database_url, &table_name).await.unwrap();

    // Setup
    setup_test_table(db_pool.pool(), &table_name).await;

    // Insert tasks in order
    let id1 = insert_task(db_pool.pool(), &table_name, "first.js").await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let _id2 = insert_task(db_pool.pool(), &table_name, "second.js").await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let _id3 = insert_task(db_pool.pool(), &table_name, "third.js").await;

    // Claim first task - should be the oldest
    let task = db_pool.claim_pending_task().await.unwrap().unwrap();
    assert_eq!(task.id, id1);
    assert_eq!(task.script, "first.js");

    // Cleanup
    cleanup_test_table(db_pool.pool(), &table_name).await;
}

#[tokio::test]
async fn test_db_channel_name() {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    };

    let db_pool = DbPool::connect(&database_url, "ow_tasks").await.unwrap();
    assert_eq!(db_pool.channel_name(), "ow_tasks_created");

    let db_pool = DbPool::connect(&database_url, "public.my_tasks")
        .await
        .unwrap();
    assert_eq!(db_pool.channel_name(), "public_my_tasks_created");
}
