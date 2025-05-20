mod cemetery;
mod config;
mod coordinator;
mod error;
mod executor;
mod job_envelope;
mod queue;
mod redis_helper;
mod retrying;
mod scheduling;
mod semaphores_map;
mod throttler;
mod timed_migrator;
mod worker;
mod worker_event;
mod worker_registry;
mod worker_state;

use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::Mutex;

pub use crate::config::Config;
use crate::coordinator::Stats;
pub use crate::error::OxanusError;
pub use crate::job_envelope::JobEnvelope;
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

    tokio::spawn(scheduling::run(redis_client.clone()));
    tokio::spawn(retrying::run(redis_client.clone()));

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
) -> Result<i64, OxanusError>
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
) -> Result<i64, OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let mut redis = redis.clone();
    let queue_key = queue.key();

    let envelope = JobEnvelope::new(queue_key.clone(), job)?;

    if delay > 0 {
        scheduling::enqueue_in(&redis, envelope, delay).await?;
    } else {
        let envelope_str = serde_json::to_string(&envelope)?;
        let _: i32 = redis.rpush(queue_key, envelope_str).await?;
    }

    Ok(0)
}
