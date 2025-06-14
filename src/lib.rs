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
