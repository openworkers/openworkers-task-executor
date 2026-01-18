//! OpenWorkers Task Executor CLI
//!
//! A minimal CLI to execute JavaScript/TypeScript tasks using V8.
//!
//! Usage:
//!   task-executor run script.js              # Execute a script once
//!   task-executor run script.js --payload '{"key": "value"}'
//!   task-executor listen --nats nats://localhost:4222 --subject tasks --root ./workers

use clap::{Parser, Subcommand};
use openworkers_core::{Event, RuntimeLimits, Script, TaskSource, WorkerCode};
use openworkers_task_executor::MinimalOps;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "task-executor")]
#[command(about = "Minimal task executor for OpenWorkers")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a JavaScript file as a task (one-shot)
    Run {
        /// Path to the script file
        script: PathBuf,

        /// JSON payload to pass to the task
        #[arg(long)]
        payload: Option<String>,

        /// Task source origin (cli, api, etc.)
        #[arg(long, default_value = "cli")]
        origin: String,

        /// Maximum execution time in milliseconds
        #[arg(long, default_value = "30000")]
        timeout: u64,

        /// Suppress console.log output
        #[arg(long)]
        quiet: bool,
    },

    /// Listen for tasks on a NATS subject
    #[cfg(feature = "nats")]
    Listen {
        /// NATS server URL
        #[arg(long, default_value = "nats://localhost:4222")]
        nats: String,

        /// NATS subject to listen on
        #[arg(long, default_value = "tasks")]
        subject: String,

        /// Root directory for scripts (sandbox)
        #[arg(long, default_value = ".")]
        root: PathBuf,

        /// Maximum execution time in milliseconds
        #[arg(long, default_value = "30000")]
        timeout: u64,

        /// Suppress console.log output
        #[arg(long)]
        quiet: bool,
    },

    /// Listen for tasks from a PostgreSQL database queue
    #[cfg(feature = "database")]
    DbListen {
        /// PostgreSQL connection URL
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        /// Name of the tasks table (default: ow_tasks)
        #[arg(long, env = "TASK_TABLE", default_value = "ow_tasks")]
        table: String,

        /// Root directory for scripts (sandbox)
        #[arg(long, default_value = ".")]
        root: PathBuf,

        /// Maximum execution time in milliseconds
        #[arg(long, default_value = "30000")]
        timeout: u64,

        /// Suppress console.log output
        #[arg(long)]
        quiet: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            script,
            payload,
            origin,
            timeout,
            quiet,
        } => {
            run_task(script, payload, origin, timeout, quiet).await?;
        }

        #[cfg(feature = "nats")]
        Commands::Listen {
            nats,
            subject,
            root,
            timeout,
            quiet,
        } => {
            listen_nats(nats, subject, root, timeout, quiet).await?;
        }

        #[cfg(feature = "database")]
        Commands::DbListen {
            database_url,
            table,
            root,
            timeout,
            quiet,
        } => {
            listen_database(database_url, table, root, timeout, quiet).await?;
        }
    }

    Ok(())
}

async fn run_task(
    script_path: PathBuf,
    payload: Option<String>,
    origin: String,
    timeout: u64,
    quiet: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let script_content = std::fs::read_to_string(&script_path)?;

    let is_typescript = script_path
        .extension()
        .map(|ext| ext == "ts" || ext == "tsx")
        .unwrap_or(false);

    if is_typescript {
        eprintln!("Warning: TypeScript files are not yet supported. Please use JavaScript.");
        return Err("TypeScript not supported".into());
    }

    let payload_value = payload
        .map(|p| serde_json::from_str(&p))
        .transpose()
        .map_err(|e| format!("Invalid JSON payload: {}", e))?;

    let result = execute_script(&script_content, payload_value, &origin, timeout, quiet).await;

    match result {
        Ok(task_result) => {
            if task_result.success {
                if let Some(data) = task_result.data {
                    println!("{}", serde_json::to_string_pretty(&data)?);
                }
            } else {
                eprintln!(
                    "Task failed: {}",
                    task_result
                        .error
                        .unwrap_or_else(|| "Unknown error".to_string())
                );
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Execution failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

#[cfg(feature = "nats")]
async fn listen_nats(
    nats_url: String,
    subject: String,
    root: PathBuf,
    timeout: u64,
    quiet: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt;

    // Canonicalize root path for security checks
    let root = root.canonicalize().map_err(|e| {
        format!(
            "Root directory '{}' does not exist or is not accessible: {}",
            root.display(),
            e
        )
    })?;

    log::info!("Connecting to NATS at {}", nats_url);
    let client = async_nats::connect(&nats_url).await?;

    log::info!(
        "Listening on subject '{}' (root: {})",
        subject,
        root.display()
    );
    let mut subscriber = client.subscribe(subject.clone()).await?;

    println!(
        "Task executor ready. Waiting for messages on '{}'...",
        subject
    );

    while let Some(msg) = subscriber.next().await {
        let reply = msg.reply.clone();

        match handle_nats_message(&client, &msg.payload, &root, timeout, quiet, reply).await {
            Ok(_) => log::debug!("Task completed successfully"),
            Err(e) => log::error!("Task failed: {}", e),
        }
    }

    Ok(())
}

/// NATS message format
#[derive(serde::Deserialize)]
struct TaskMessage {
    /// Path to script file (relative to root)
    script: String,
    /// JSON payload for the task
    payload: Option<serde_json::Value>,
    /// Timeout in milliseconds (optional, uses default if not specified)
    timeout: Option<u64>,
}

#[cfg(feature = "nats")]
async fn handle_nats_message(
    client: &async_nats::Client,
    payload: &[u8],
    root: &PathBuf,
    default_timeout: u64,
    quiet: bool,
    reply: Option<async_nats::Subject>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse the message
    let msg: TaskMessage =
        serde_json::from_slice(payload).map_err(|e| format!("Invalid message format: {}", e))?;

    // Get script content from file
    let resolved = resolve_script_path(root, &msg.script)?;
    let script_content = std::fs::read_to_string(&resolved)
        .map_err(|e| format!("Failed to read script '{}': {}", msg.script, e))?;

    let timeout = msg.timeout.unwrap_or(default_timeout);

    // Execute the task
    let result = execute_script(&script_content, msg.payload, "nats", timeout, quiet).await;

    // Send reply if requested
    if let Some(reply_subject) = reply {
        let response = match result {
            Ok(task_result) => task_result,
            Err(e) => openworkers_core::TaskResult::err(e.to_string()),
        };

        let response_bytes = serde_json::to_vec(&response)?;
        client.publish(reply_subject, response_bytes.into()).await?;
    }

    Ok(())
}

/// Resolve script path safely within the root directory
fn resolve_script_path(root: &PathBuf, script_path: &str) -> Result<PathBuf, String> {
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

/// Execute a script and return the result
#[cfg(feature = "v8")]
async fn execute_script(
    script_content: &str,
    payload: Option<serde_json::Value>,
    origin: &str,
    timeout: u64,
    quiet: bool,
) -> Result<openworkers_core::TaskResult, Box<dyn std::error::Error>> {
    use openworkers_runtime_v8::Worker;

    let script = Script {
        code: WorkerCode::JavaScript(script_content.to_string()),
        env: None,
        bindings: vec![],
    };

    let ops: openworkers_core::OperationsHandle = Arc::new(if quiet {
        MinimalOps::silent()
    } else {
        MinimalOps::new()
    });

    let limits = RuntimeLimits {
        max_wall_clock_time_ms: timeout,
        max_cpu_time_ms: timeout,
        ..Default::default()
    };

    let task_id = uuid::Uuid::new_v4().to_string();
    let source = TaskSource::Invoke {
        origin: Some(origin.to_string()),
    };

    let (event, rx) = Event::task(task_id, payload, Some(source), 1);

    let local = tokio::task::LocalSet::new();

    let result = local
        .run_until(async move {
            let mut worker = Worker::new_with_ops(script, Some(limits), ops).await?;
            worker.exec(event).await?;
            Ok::<_, openworkers_core::TerminationReason>(())
        })
        .await;

    match result {
        Ok(()) => {
            let task_result = rx.await.map_err(|_| "Task channel closed")?;
            Ok(task_result)
        }
        Err(reason) => Err(format!("Execution failed: {:?}", reason).into()),
    }
}

#[cfg(not(feature = "v8"))]
async fn execute_script(
    _script_content: &str,
    _payload: Option<serde_json::Value>,
    _origin: &str,
    _timeout: u64,
    _quiet: bool,
) -> Result<openworkers_core::TaskResult, Box<dyn std::error::Error>> {
    Err("V8 runtime not enabled. Build with --features v8".into())
}

#[cfg(feature = "database")]
async fn listen_database(
    database_url: String,
    table_name: String,
    root: PathBuf,
    timeout: u64,
    quiet: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use openworkers_task_executor::db::{DbListenerConfig, listen_loop};

    let config = DbListenerConfig {
        database_url,
        table_name,
        root,
        timeout,
        quiet,
    };

    // Create an executor function that bridges async boundaries
    let execute_fn =
        move |script: &str, payload: Option<serde_json::Value>, timeout: u64, quiet: bool| {
            let script = script.to_string();

            Box::pin(async move { execute_script_for_db(&script, payload, timeout, quiet).await })
                as std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<
                                    openworkers_core::TaskResult,
                                    Box<dyn std::error::Error>,
                                >,
                            > + Send,
                    >,
                >
        };

    listen_loop(config, execute_fn).await
}

/// Execute script variant for database listener (needs to be Send)
#[cfg(all(feature = "database", feature = "v8"))]
async fn execute_script_for_db(
    script_content: &str,
    payload: Option<serde_json::Value>,
    timeout: u64,
    quiet: bool,
) -> Result<openworkers_core::TaskResult, Box<dyn std::error::Error>> {
    use openworkers_runtime_v8::Worker;

    let script = Script {
        code: WorkerCode::JavaScript(script_content.to_string()),
        env: None,
        bindings: vec![],
    };

    let ops: openworkers_core::OperationsHandle = Arc::new(if quiet {
        MinimalOps::silent()
    } else {
        MinimalOps::new()
    });

    let limits = RuntimeLimits {
        max_wall_clock_time_ms: timeout,
        max_cpu_time_ms: timeout,
        ..Default::default()
    };

    let task_id = uuid::Uuid::new_v4().to_string();
    let source = TaskSource::Invoke {
        origin: Some("database".to_string()),
    };

    let (event, rx) = Event::task(task_id, payload, Some(source), 1);

    // Spawn a blocking task to handle the !Send V8 runtime
    let result = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create runtime");

        let local = tokio::task::LocalSet::new();

        local.block_on(&rt, async move {
            let mut worker = Worker::new_with_ops(script, Some(limits), ops).await?;
            worker.exec(event).await?;
            Ok::<_, openworkers_core::TerminationReason>(())
        })
    })
    .await
    .map_err(|e| format!("Task join error: {}", e))?;

    match result {
        Ok(()) => {
            let task_result = rx.await.map_err(|_| "Task channel closed")?;
            Ok(task_result)
        }
        Err(reason) => Err(format!("Execution failed: {:?}", reason).into()),
    }
}

#[cfg(all(feature = "database", not(feature = "v8")))]
async fn execute_script_for_db(
    _script_content: &str,
    _payload: Option<serde_json::Value>,
    _timeout: u64,
    _quiet: bool,
) -> Result<openworkers_core::TaskResult, Box<dyn std::error::Error>> {
    Err("V8 runtime not enabled. Build with --features v8,database".into())
}
