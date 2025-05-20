mod config;
mod coordinator;
mod error;
mod executor;
mod job_envelope;
mod queue;
mod semaphores_map;
mod storage;
mod throttler;
mod timed_migrator;
mod worker;
mod worker_event;
mod worker_registry;
mod worker_state;

use std::sync::Arc;
use tokio::sync::Mutex;

pub use crate::config::Config;
use crate::coordinator::Stats;
pub use crate::error::OxanusError;
pub use crate::job_envelope::{Job, JobEnvelope};
pub use crate::queue::{
    Queue, QueueConfig, QueueKind, QueueRetry, QueueRetryBackoff, QueueThrottle,
};
pub use crate::worker::Worker;
pub use crate::worker_state::WorkerState;

pub async fn run<
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    redis_client: &redis::Client,
    config: Config<DT, ET>,
    data: WorkerState<DT>,
) -> Result<Stats, OxanusError> {
    let config = Arc::new(config);
    let mut joinset = tokio::task::JoinSet::new();
    let stats = Arc::new(Mutex::new(Stats::default()));

    tokio::spawn(timed_migrator::run(
        redis_client.clone(),
        storage::SCHEDULE_QUEUE.to_string(),
    ));
    tokio::spawn(timed_migrator::run(
        redis_client.clone(),
        storage::RETRY_QUEUE.to_string(),
    ));

    for queue_config in &config.queues {
        joinset.spawn(coordinator::run(
            redis_client.clone(),
            config.clone(),
            stats.clone(),
            data.clone(),
            queue_config.clone(),
        ));
    }

    joinset.join_all().await;

    let stats = Arc::try_unwrap(stats)
        .expect("Failed to unwrap Arc - there are still references to stats")
        .into_inner();

    Ok(stats)
}

pub async fn enqueue<
    T,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    redis: &redis::aio::ConnectionManager,
    queue: impl Queue,
    job: T,
) -> Result<(), OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    enqueue_in(redis, queue, job, 0).await
}

pub async fn enqueue_in<
    T,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    redis: &redis::aio::ConnectionManager,
    queue: impl Queue,
    job: T,
    delay: u64,
) -> Result<(), OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let envelope = JobEnvelope::new(queue.key().clone(), job)?;

    if delay > 0 {
        storage::enqueue_in(&redis, &envelope, delay).await
    } else {
        storage::enqueue(&redis, &envelope).await
    }
}
