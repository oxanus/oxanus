mod config;
mod coordinator;
mod dispatcher;
mod error;
mod executor;
mod job_envelope;
mod queue;
mod result_collector;
mod semaphores_map;
mod storage;
mod throttler;
mod worker;
mod worker_context;
mod worker_event;
mod worker_registry;

#[cfg(test)]
mod test_helper;

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;
pub use crate::error::OxanusError;
pub use crate::job_envelope::{Job, JobEnvelope, JobId};
pub use crate::queue::{Queue, QueueConfig, QueueKind, QueueThrottle};
pub use crate::result_collector::Stats;
pub use crate::storage::Storage;
pub use crate::worker::Worker;
pub use crate::worker_context::{WorkerContext, WorkerContextValue};
use crate::worker_registry::CronJob;

pub async fn run<DT, ET>(
    config: Config<DT, ET>,
    ctx: WorkerContextValue<DT>,
) -> Result<Stats, OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
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
    config.storage.retry_loop(config.cancel_token.clone()).await
}

async fn schedule_loop<DT, ET>(config: Arc<Config<DT, ET>>) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config
        .storage
        .schedule_loop(config.cancel_token.clone())
        .await
}

async fn ping_loop<DT, ET>(config: Arc<Config<DT, ET>>) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config.storage.ping_loop(config.cancel_token.clone()).await
}

async fn resurrect_loop<DT, ET>(config: Arc<Config<DT, ET>>) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config
        .storage
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
            config.storage.clone(),
            config.cancel_token.clone(),
            name.clone(),
            cron_job.clone(),
        ));
    }

    Ok(())
}

async fn cron_job_loop(
    storage: Storage,
    cancel_token: CancellationToken,
    job_name: String,
    cron_job: CronJob,
) -> Result<(), OxanusError> {
    storage
        .cron_job_loop(cancel_token, job_name, cron_job)
        .await
}

pub async fn enqueue<T, DT, ET>(
    storage: &Storage,
    queue: impl Queue,
    job: T,
) -> Result<JobId, OxanusError>
where
    T: Worker<Context = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    enqueue_in(storage, queue, job, 0).await
}

pub async fn enqueue_in<T, DT, ET>(
    storage: &Storage,
    queue: impl Queue,
    job: T,
    delay: u64,
) -> Result<JobId, OxanusError>
where
    T: Worker<Context = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let envelope = JobEnvelope::new(queue.key().clone(), job)?;

    if delay > 0 {
        storage.enqueue_in(envelope, delay).await
    } else {
        storage.enqueue(envelope).await
    }
}
