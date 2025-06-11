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
mod worker_event;
mod worker_registry;
mod worker_state;

#[cfg(test)]
mod test_helper;

use std::sync::Arc;
use tokio::sync::Mutex;

pub use crate::config::Config;
pub use crate::error::OxanusError;
pub use crate::job_envelope::{Job, JobEnvelope, JobId};
pub use crate::queue::{Queue, QueueConfig, QueueKind, QueueThrottle};
pub use crate::result_collector::Stats;
pub use crate::storage::Storage;
pub use crate::worker::Worker;
pub use crate::worker_state::WorkerState;

pub async fn run<DT, ET>(
    config: Config<DT, ET>,
    data: WorkerState<DT>,
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

    for queue_config in &config.queues {
        joinset.spawn(coordinator::run(
            config.clone(),
            stats.clone(),
            data.clone(),
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

pub async fn enqueue<T, DT, ET>(
    config: &Config<DT, ET>,
    queue: impl Queue,
    job: T,
) -> Result<JobId, OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    enqueue_in(config, queue, job, 0).await
}

pub async fn enqueue_in<T, DT, ET>(
    config: &Config<DT, ET>,
    queue: impl Queue,
    job: T,
    delay: u64,
) -> Result<JobId, OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let envelope = JobEnvelope::new(queue.key().clone(), job)?;

    if delay > 0 {
        config.storage.enqueue_in(envelope, delay).await
    } else {
        config.storage.enqueue(envelope).await
    }
}
