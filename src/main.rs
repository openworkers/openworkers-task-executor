//! OpenWorkers Task Executor CLI
//!
//! A minimal CLI to execute JavaScript/TypeScript tasks using V8.
//!
//! Usage:
//!   task-executor run script.js              # Execute a script
//!   task-executor run script.js --payload '{"key": "value"}'

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
    /// Run a JavaScript/TypeScript file as a task
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
    // Read script file
    let script_content = std::fs::read_to_string(&script_path)?;

    // Detect if TypeScript (would need transpilation)
    let is_typescript = script_path
        .extension()
        .map(|ext| ext == "ts" || ext == "tsx")
        .unwrap_or(false);

    if is_typescript {
        eprintln!("Warning: TypeScript files are not yet supported. Please use JavaScript.");
        return Err("TypeScript not supported".into());
    }

    // Create script
    let script = Script {
        code: WorkerCode::JavaScript(script_content),
        env: None,
        bindings: vec![],
    };

    // Create ops handler
    let ops: openworkers_core::OperationsHandle = Arc::new(if quiet {
        MinimalOps::silent()
    } else {
        MinimalOps::new()
    });

    // Create limits
    let limits = RuntimeLimits {
        max_wall_clock_time_ms: timeout,
        max_cpu_time_ms: timeout,
        ..Default::default()
    };

    // Generate task ID
    let task_id = uuid::Uuid::new_v4().to_string();

    // Parse payload
    let payload_value = payload
        .map(|p| serde_json::from_str(&p))
        .transpose()
        .map_err(|e| format!("Invalid JSON payload: {}", e))?;

    // Create task
    let source = TaskSource::Invoke {
        origin: Some(origin),
    };

    let (event, rx) = Event::task(task_id.clone(), payload_value, Some(source), 1);

    // Execute using V8 runtime
    #[cfg(feature = "v8")]
    {
        use openworkers_runtime_v8::Worker;

        // Must run in a LocalSet for V8
        let local = tokio::task::LocalSet::new();

        let result = local
            .run_until(async move {
                let mut worker = Worker::new_with_ops(script, Some(limits), ops).await?;

                worker.exec(event).await
            })
            .await;

        match result {
            Ok(()) => {
                // Wait for result from the task
                match rx.await {
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
                    Err(_) => {
                        eprintln!("Task channel closed without result");
                        std::process::exit(1);
                    }
                }
            }
            Err(reason) => {
                eprintln!("Execution failed: {:?}", reason);
                std::process::exit(1);
            }
        }
    }

    #[cfg(not(feature = "v8"))]
    {
        eprintln!("V8 runtime not enabled. Build with --features v8");
        std::process::exit(1);
    }

    Ok(())
}
