//! Oxanus is job processing library written in Rust doesn't suck (or at least sucks in a completely different way than other options).
//!
//! Oxanus goes for simplity and depth over breadth. It only aims support single backend with simple flow.
//!
//! # Key Features
//!
//! - **Isolated Queues**: Separate job processing queues with independent configurations
//! - **Retrying**: Automatic retry of failed jobs with configurable backoff
//! - **Scheduled Jobs**: Schedule jobs to run at specific times or after delays
//! - **Dynamic Queues**: Create and manage queues at runtime
//! - **Throttling**: Control job processing rates with queue-based throttling
//! - **Unique Jobs**: Ensure only one instance of a job runs at a time
//! - **Resilient Jobs**: Jobs that can survive worker crashes and restarts
//! - **Graceful Shutdown**: Clean shutdown of workers with in-progress job handling
//! - **Periodic Jobs**: Run jobs on a schedule using cron-like expressions
//!
//! # Quick Start
//!
//! ```rust
//! use oxanus::{Worker, Queue, Context, Config, Storage};
//! use serde::{Serialize, Deserialize};
//!
//! // Define your worker
//! #[derive(Debug, Serialize, Deserialize)]
//! struct MyWorker {
//!     data: String,
//! }
//!
//! #[async_trait::async_trait]
//! impl Worker for MyWorker {
//!     type Context = MyContext;
//!     type Error = MyError;
//!
//!     async fn process(&self, ctx: &Context<MyContext>) -> Result<(), MyError> {
//!         // Process your job here
//!         Ok(())
//!     }
//! }
//!
//! // Define your queue
//! #[derive(Serialize)]
//! struct MyQueue;
//!
//! impl Queue for MyQueue {
//!     fn to_config() -> QueueConfig {
//!         QueueConfig {
//!             kind: QueueKind::Static {
//!                 key: "my_queue".to_string(),
//!             },
//!             concurrency: 2,
//!             throttle: None,
//!         }
//!     }
//! }
//!
//! // Run your worker
//! async fn run_worker() -> Result<(), OxanusError> {
//!     let ctx = Context::value(MyContext {});
//!     let storage = Storage::builder().from_env()?.build()?;
//!     let config = Config::new(&storage)
//!         .register_queue::<MyQueue>()
//!         .register_worker::<MyWorker>()
//!         .with_graceful_shutdown(tokio::signal::ctrl_c());
//!
//!     // Enqueue some jobs
//!     storage.enqueue(MyQueue, MyWorker { data: "hello".into() }).await?;
//!
//!     // Run the worker
//!     oxanus::run(config, ctx).await?;
//!     Ok(())
//! }
//! ```
//!
//! # Core Concepts
//!
//! ## Workers
//!
//! Workers are the units of work in Oxanus. They implement the [`Worker`] trait and define the processing logic.
//!
//! ## Queues
//!
//! Queues are the channels through which jobs flow. They can be:
//!
//! - Static: Defined at compile time
//! - Dynamic: Created at runtime with each instance being a separate queue
//!
//! Each queue can have its own:
//!
//! - Concurrency limits
//! - Throttling rules
//! - Retry policies
//!
//! ## Storage
//!
//! The [`Storage`] trait provides the interface for job persistence. It handles:
//! - Job enqueueing
//! - Job scheduling
//! - Job state management
//! - Queue monitoring
//!
//! ## Context
//!
//! The [`Context`] provides shared state and utilities to workers. It can include:
//! - Database connections
//! - Configuration
//! - Shared resources
//!
//! # Examples
//!
//! Check out the [examples directory](https://github.com/oxanus/oxanus/tree/main/examples) for more detailed usage examples:
//!
//! # Error Handling
//!
//! Oxanus uses a custom error type [`OxanusError`] that covers all possible error cases in the library.
//! Workers can define their own error types that implement `std::error::Error`.
//!
//! # Configuration
//!
//! Configuration is done through the [`Config`] builder, which allows you to:
//! - Register queues and workers
//! - Set up graceful shutdown
//! - Configure periodic jobs
//! - Customize worker behavior

mod config;
mod context;
mod coordinator;
mod dispatcher;
mod error;
mod executor;
mod job_envelope;
mod queue;
mod result_collector;
mod semaphores_map;
mod storage;
mod storage_builder;
mod storage_internal;
mod throttler;
mod worker;
mod worker_event;
mod worker_registry;

#[cfg(test)]
mod test_helper;

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;
pub use crate::context::Context;
pub use crate::error::OxanusError;
pub use crate::job_envelope::JobId;
pub use crate::queue::{Queue, QueueConfig, QueueKind, QueueThrottle};
pub use crate::storage::Storage;
pub use crate::storage_builder::StorageBuilder;
pub use crate::worker::Worker;

use crate::context::ContextValue;
use crate::job_envelope::JobEnvelope;
use crate::result_collector::Stats;
use crate::storage_internal::StorageInternal;
use crate::worker_registry::CronJob;

/// Runs the Oxanus worker system with the given configuration and context.
///
/// This is the main entry point for running Oxanus workers. It sets up all necessary
/// background tasks and starts processing jobs from the configured queues.
///
/// # Arguments
///
/// * `config` - The worker configuration, including queue and worker registrations
/// * `ctx` - The context value that will be shared across all worker instances
///
/// # Returns
///
/// Returns statistics about the worker run, or an [`OxanusError`] if the operation fails.
///
/// # Examples
///
/// ```rust
/// use oxanus::{Config, Context, Storage, Queue, Worker};
///
/// async fn run_worker() -> Result<(), oxanus::OxanusError> {
///     let ctx = Context::value(MyContext {});
///     let storage = Storage::builder().from_env()?.build()?;
///     
///     let config = Config::new(&storage)
///         .register_queue::<MyQueue>()
///         .register_worker::<MyWorker>()
///         .with_graceful_shutdown(tokio::signal::ctrl_c());
///     
///     let stats = oxanus::run(config, ctx).await?;
///     println!("Processed {} jobs", stats.processed);
///     
///     Ok(())
/// }
/// ```
pub async fn run<DT, ET>(
    config: Config<DT, ET>,
    ctx: ContextValue<DT>,
) -> Result<Stats, OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    tracing::info!(
        "Starting worker (namespace: {})",
        config.storage.namespace()
    );

    let mut config = config;
    let shutdown_signal = config.consume_shutdown_signal();
    let config = Arc::new(config);
    let mut joinset = tokio::task::JoinSet::new();
    let stats = Arc::new(Mutex::new(Stats::default()));

    tokio::spawn(retry_loop(config.clone()));
    tokio::spawn(schedule_loop(config.clone()));
    tokio::spawn(ping_loop(config.clone()));
    tokio::spawn(resurrect_loop(config.clone()));
    tokio::spawn(cron_loop(config.clone()));

    for queue_config in &config.queues {
        joinset.spawn(coordinator::run(
            config.clone(),
            stats.clone(),
            ctx.clone(),
            queue_config.clone(),
        ));
    }

    tokio::select! {
        _ = config.cancel_token.cancelled() => {}
        _ = shutdown_signal => {
            config.cancel_token.cancel();
        }
    }

    tracing::info!("Shutting down");

    joinset.join_all().await;

    let stats = Arc::try_unwrap(stats)
        .expect("Failed to unwrap Arc - there are still references to stats")
        .into_inner();

    tracing::info!("Gracefully shut down");

    Ok(stats)
}

async fn retry_loop<DT, ET>(config: Arc<Config<DT, ET>>) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config
        .storage
        .internal
        .retry_loop(config.cancel_token.clone())
        .await
}

async fn schedule_loop<DT, ET>(config: Arc<Config<DT, ET>>) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config
        .storage
        .internal
        .schedule_loop(config.cancel_token.clone())
        .await
}

async fn ping_loop<DT, ET>(config: Arc<Config<DT, ET>>) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config
        .storage
        .internal
        .ping_loop(config.cancel_token.clone())
        .await
}

async fn resurrect_loop<DT, ET>(config: Arc<Config<DT, ET>>) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config
        .storage
        .internal
        .resurrect_loop(config.cancel_token.clone())
        .await
}

async fn cron_loop<DT, ET>(config: Arc<Config<DT, ET>>) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    for (name, cron_job) in &config.registry.schedules {
        tokio::spawn(cron_job_loop(
            config.storage.internal.clone(),
            config.cancel_token.clone(),
            name.clone(),
            cron_job.clone(),
        ));
    }

    Ok(())
}

async fn cron_job_loop(
    storage: StorageInternal,
    cancel_token: CancellationToken,
    job_name: String,
    cron_job: CronJob,
) -> Result<(), OxanusError> {
    storage
        .cron_job_loop(cancel_token, job_name, cron_job)
        .await
}
