use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::context::ContextValue;
use crate::coordinator;
use crate::error::OxanusError;
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
        .await?;

    tracing::trace!("Retry loop finished");

    Ok(())
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
        .await?;

    tracing::trace!("Schedule loop finished");

    Ok(())
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
        .await?;

    tracing::trace!("Ping loop finished");

    Ok(())
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
        .await?;

    tracing::trace!("Resurrect loop finished");

    Ok(())
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
        .cron_job_loop(cancel_token, job_name.clone(), cron_job)
        .await?;

    tracing::trace!("Cron job loop finished for {}", job_name);

    Ok(())
}
