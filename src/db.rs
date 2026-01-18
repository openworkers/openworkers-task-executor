//! Database task queue listener using PostgreSQL
//!
//! Uses `pg_notify` for real-time notifications and `SELECT FOR UPDATE SKIP LOCKED`
//! for safe concurrent task claiming.
//!
//! # Table Schema
//!
//! The table name is configurable (default: `ow_tasks`). Example schema:
//!
//! ```sql
//! CREATE TABLE ow_tasks (
//!     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//!     script TEXT NOT NULL,
//!     payload JSONB,
//!     status TEXT NOT NULL DEFAULT 'pending',
//!     result JSONB,
//!     error TEXT,
//!     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
//!     started_at TIMESTAMPTZ,
//!     completed_at TIMESTAMPTZ
//! );
//!
//! CREATE INDEX idx_ow_tasks_pending ON ow_tasks(created_at) WHERE status = 'pending';
//!
//! CREATE OR REPLACE FUNCTION notify_ow_task_created() RETURNS TRIGGER AS $$
//! BEGIN
//!     PERFORM pg_notify('ow_tasks_created', NEW.id::text);
//!     RETURN NEW;
//! END;
//! $$ LANGUAGE plpgsql;
//!
//! CREATE TRIGGER ow_task_notify_insert
//!     AFTER INSERT ON ow_tasks
//!     FOR EACH ROW EXECUTE FUNCTION notify_ow_task_created();
//! ```

use sqlx::Row;
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use sqlx::types::JsonValue;
use sqlx::types::chrono::{DateTime, Utc};
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

/// A task from the database queue
#[derive(Debug)]
pub struct DbTask {
    pub id: Uuid,
    pub script: String,
    pub payload: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
}

/// Result of a task execution to be stored in the database
pub struct TaskCompletion {
    pub result: Option<JsonValue>,
    pub error: Option<String>,
}

/// Database connection pool with configurable table name
pub struct DbPool {
    pool: PgPool,
    table_name: String,
    channel_name: String,
}

impl DbPool {
    /// Create a new database pool from a connection URL
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection URL
    /// * `table_name` - Name of the tasks table (default: "ow_tasks")
    pub async fn connect(database_url: &str, table_name: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        // Channel name follows table name pattern
        let channel_name = format!("{}_created", table_name.replace('.', "_"));

        Ok(Self {
            pool,
            table_name: table_name.to_string(),
            channel_name,
        })
    }

    /// Get the notification channel name for this table
    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    /// Claim a pending task using SELECT FOR UPDATE SKIP LOCKED
    ///
    /// This atomically:
    /// 1. Finds the oldest pending task
    /// 2. Marks it as 'running' with started_at timestamp
    /// 3. Returns the task data
    ///
    /// SKIP LOCKED ensures multiple workers don't block each other.
    pub async fn claim_pending_task(&self) -> Result<Option<DbTask>, sqlx::Error> {
        // Build query with table name
        let query = format!(
            r#"
            UPDATE {table}
            SET status = 'running', started_at = now()
            WHERE id = (
                SELECT id FROM {table}
                WHERE status = 'pending'
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, script, payload, created_at
            "#,
            table = self.table_name
        );

        let row = sqlx::query(&query).fetch_optional(&self.pool).await?;

        Ok(row.map(|r| DbTask {
            id: r.get("id"),
            script: r.get("script"),
            payload: r.get("payload"),
            created_at: r.get("created_at"),
        }))
    }

    /// Update task with completion status
    pub async fn complete_task(
        &self,
        task_id: Uuid,
        completion: TaskCompletion,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {table}
            SET status = CASE WHEN $2 IS NULL THEN 'completed' ELSE 'failed' END,
                result = $3,
                error = $2,
                completed_at = now()
            WHERE id = $1
            "#,
            table = self.table_name
        );

        sqlx::query(&query)
            .bind(task_id)
            .bind(&completion.error)
            .bind(&completion.result)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get the underlying pool for creating listeners
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Configuration for the database listener
pub struct DbListenerConfig {
    pub database_url: String,
    pub table_name: String,
    pub root: PathBuf,
    pub timeout: u64,
    pub quiet: bool,
}

/// Main listen loop that combines pg_notify with polling
///
/// Flow:
/// 1. On startup, poll all existing pending tasks
/// 2. Start LISTEN on '{table_name}_created' channel
/// 3. For each notification, try to claim and execute a task
/// 4. Periodically poll for missed tasks (in case notifications are lost)
pub async fn listen_loop(
    config: DbListenerConfig,
    execute_fn: impl Fn(
        &str,
        Option<serde_json::Value>,
        u64,
        bool,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<openworkers_core::TaskResult, Box<dyn std::error::Error>>,
                > + Send,
        >,
    > + Send
    + Sync
    + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    // Canonicalize root path for security
    let root = config.root.canonicalize().map_err(|e| {
        format!(
            "Root directory '{}' does not exist or is not accessible: {}",
            config.root.display(),
            e
        )
    })?;

    log::info!("Connecting to database...");
    let pool = DbPool::connect(&config.database_url, &config.table_name).await?;

    log::info!(
        "Setting up pg_notify listener on channel '{}'...",
        pool.channel_name()
    );
    let mut listener = PgListener::connect_with(pool.pool()).await?;
    listener.listen(pool.channel_name()).await?;

    let pool = Arc::new(pool);
    let execute_fn = Arc::new(execute_fn);

    println!(
        "Database task executor ready. Table: '{}', root: {}",
        config.table_name,
        root.display()
    );

    // Initial poll for existing pending tasks
    log::info!("Processing existing pending tasks...");
    process_pending_tasks(&pool, &root, &config, &execute_fn).await;

    // Main loop: wait for notifications
    loop {
        // Wait for a notification with timeout for periodic polling
        let notification =
            tokio::time::timeout(std::time::Duration::from_secs(30), listener.recv()).await;

        match notification {
            Ok(Ok(notif)) => {
                log::debug!("Received notification for task: {}", notif.payload());

                // Process the specific task or any pending task
                process_pending_tasks(&pool, &root, &config, &execute_fn).await;
            }

            Ok(Err(e)) => {
                log::error!("Listener error: {}. Reconnecting...", e);

                // Try to reconnect
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                listener = PgListener::connect_with(pool.pool()).await?;
                listener.listen(pool.channel_name()).await?;
            }

            Err(_) => {
                // Timeout - periodic poll for missed tasks
                log::debug!("Periodic poll for pending tasks...");
                process_pending_tasks(&pool, &root, &config, &execute_fn).await;
            }
        }
    }
}

/// Process all pending tasks until none remain
async fn process_pending_tasks<F>(
    pool: &Arc<DbPool>,
    root: &PathBuf,
    config: &DbListenerConfig,
    execute_fn: &Arc<F>,
) where
    F: Fn(
            &str,
            Option<serde_json::Value>,
            u64,
            bool,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<openworkers_core::TaskResult, Box<dyn std::error::Error>>,
                    > + Send,
            >,
        > + Send
        + Sync
        + 'static,
{
    loop {
        match pool.claim_pending_task().await {
            Ok(Some(task)) => {
                log::info!("Claimed task: {}", task.id);
                process_task(pool, root, config, execute_fn, task).await;
            }

            Ok(None) => {
                // No more pending tasks
                break;
            }

            Err(e) => {
                log::error!("Failed to claim task: {}", e);
                break;
            }
        }
    }
}

/// Process a single claimed task
async fn process_task<F>(
    pool: &Arc<DbPool>,
    root: &PathBuf,
    config: &DbListenerConfig,
    execute_fn: &Arc<F>,
    task: DbTask,
) where
    F: Fn(
            &str,
            Option<serde_json::Value>,
            u64,
            bool,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<openworkers_core::TaskResult, Box<dyn std::error::Error>>,
                    > + Send,
            >,
        > + Send
        + Sync
        + 'static,
{
    // Get script content
    let script_content = match get_script_content(root, &task) {
        Ok(content) => content,

        Err(e) => {
            log::error!("Task {}: Failed to get script: {}", task.id, e);

            let _ = pool
                .complete_task(
                    task.id,
                    TaskCompletion {
                        result: None,
                        error: Some(e),
                    },
                )
                .await;

            return;
        }
    };

    // Convert payload
    let payload: Option<serde_json::Value> = task
        .payload
        .map(|v| serde_json::from_str(&v.to_string()).unwrap_or(serde_json::Value::Null));

    // Execute the task
    let result = execute_fn(&script_content, payload, config.timeout, config.quiet).await;

    // Update database with result
    let completion = match result {
        Ok(task_result) => {
            if task_result.success {
                log::info!("Task {} completed successfully", task.id);

                TaskCompletion {
                    result: task_result
                        .data
                        .map(|d| serde_json::from_str(&d.to_string()).unwrap_or(JsonValue::Null)),
                    error: None,
                }
            } else {
                let error = task_result
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string());
                log::warn!("Task {} failed: {}", task.id, error);

                TaskCompletion {
                    result: None,
                    error: Some(error),
                }
            }
        }

        Err(e) => {
            log::error!("Task {} execution error: {}", task.id, e);

            TaskCompletion {
                result: None,
                error: Some(e.to_string()),
            }
        }
    };

    if let Err(e) = pool.complete_task(task.id, completion).await {
        log::error!("Failed to update task {}: {}", task.id, e);
    }
}

/// Get script content from file
pub(crate) fn get_script_content(root: &PathBuf, task: &DbTask) -> Result<String, String> {
    let resolved = resolve_script_path(root, &task.script)?;

    std::fs::read_to_string(&resolved)
        .map_err(|e| format!("Failed to read script '{}': {}", task.script, e))
}

/// Resolve script path safely within the root directory
pub(crate) fn resolve_script_path(root: &PathBuf, script_path: &str) -> Result<PathBuf, String> {
    // Reject absolute paths
    if script_path.starts_with('/') || script_path.starts_with('\\') {
        return Err(format!("Absolute paths are not allowed: '{}'", script_path));
    }

    // Reject obvious path traversal attempts
    if script_path.contains("..") {
        return Err(format!("Path traversal not allowed: '{}'", script_path));
    }

    // Build the full path
    let full_path = root.join(script_path);

    // Canonicalize to resolve any remaining tricks
    let canonical = full_path
        .canonicalize()
        .map_err(|e| format!("Script not found: '{}' ({})", script_path, e))?;

    // Verify it's still within root
    if !canonical.starts_with(root) {
        return Err(format!(
            "Script path escapes root directory: '{}'",
            script_path
        ));
    }

    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn make_task(script: &str) -> DbTask {
        DbTask {
            id: Uuid::new_v4(),
            script: script.to_string(),
            payload: None,
            created_at: Utc::now(),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // get_script_content tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_get_script_content_from_file() {
        let temp_dir = TempDir::new().unwrap();
        // Canonicalize to handle macOS /tmp -> /private/tmp symlink
        let root = temp_dir.path().canonicalize().unwrap();
        let script_path = root.join("test.js");
        fs::write(&script_path, "export default {}").unwrap();

        let task = make_task("test.js");
        let content = get_script_content(&root, &task).unwrap();

        assert_eq!(content, "export default {}");
    }

    #[test]
    fn test_get_script_content_nested_file() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().canonicalize().unwrap();
        let nested_dir = root.join("workers");
        fs::create_dir(&nested_dir).unwrap();
        let script_path = nested_dir.join("task.js");
        fs::write(&script_path, "console.log('nested')").unwrap();

        let task = make_task("workers/task.js");
        let content = get_script_content(&root, &task).unwrap();

        assert_eq!(content, "console.log('nested')");
    }

    #[test]
    fn test_get_script_content_file_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().canonicalize().unwrap();
        let task = make_task("nonexistent.js");

        let result = get_script_content(&root, &task);
        assert!(result.is_err());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // resolve_script_path tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_resolve_script_path_valid() {
        let temp_dir = TempDir::new().unwrap();
        // Canonicalize to handle macOS /tmp -> /private/tmp symlink
        let root = temp_dir.path().canonicalize().unwrap();
        let script_path = root.join("worker.js");
        fs::write(&script_path, "").unwrap();

        let result = resolve_script_path(&root, "worker.js");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), script_path);
    }

    #[test]
    fn test_resolve_script_path_nested() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().canonicalize().unwrap();
        let nested_dir = root.join("workers");
        fs::create_dir(&nested_dir).unwrap();
        let script_path = nested_dir.join("task.js");
        fs::write(&script_path, "").unwrap();

        let result = resolve_script_path(&root, "workers/task.js");
        assert!(result.is_ok());
    }

    #[test]
    fn test_resolve_script_path_absolute_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().canonicalize().unwrap();

        let result = resolve_script_path(&root, "/etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Absolute paths"));
    }

    #[test]
    fn test_resolve_script_path_traversal_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().canonicalize().unwrap();

        let result = resolve_script_path(&root, "../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("traversal"));
    }

    #[test]
    fn test_resolve_script_path_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().canonicalize().unwrap();

        let result = resolve_script_path(&root, "missing.js");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DbPool channel name tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_channel_name_from_table() {
        // Test the channel name generation logic
        let table_name = "ow_tasks";
        let expected = "ow_tasks_created";
        let channel = format!("{}_created", table_name.replace('.', "_"));

        assert_eq!(channel, expected);
    }

    #[test]
    fn test_channel_name_with_schema() {
        // Table with schema prefix
        let table_name = "public.my_tasks";
        let expected = "public_my_tasks_created";
        let channel = format!("{}_created", table_name.replace('.', "_"));

        assert_eq!(channel, expected);
    }
}
