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

use futures::stream::StreamExt;
use signal_hook_tokio::Signals;
use std::sync::Arc;
use tokio::select;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;
pub use crate::error::OxanusError;
pub use crate::job_envelope::{Job, JobEnvelope};
pub use crate::queue::{Queue, QueueConfig, QueueKind, QueueThrottle};
use crate::result_collector::Stats;
pub use crate::worker::Worker;
pub use crate::worker_state::WorkerState;

pub async fn run<DT, ET>(
    redis_client: &redis::Client,
    config: Config<DT, ET>,
    data: WorkerState<DT>,
) -> Result<Stats, OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let config = Arc::new(config);
    let mut joinset = tokio::task::JoinSet::new();
    let stats = Arc::new(Mutex::new(Stats::default()));
    let cancel_token = CancellationToken::new();

    tokio::spawn(storage::retry_loop(
        redis_client.clone(),
        cancel_token.clone(),
    ));
    tokio::spawn(storage::schedule_loop(
        redis_client.clone(),
        cancel_token.clone(),
    ));
    tokio::spawn(storage::ping_loop(
        redis_client.clone(),
        cancel_token.clone(),
    ));
    tokio::spawn(storage::resurrect_loop(
        redis_client.clone(),
        cancel_token.clone(),
    ));

    for queue_config in &config.queues {
        joinset.spawn(coordinator::run(
            redis_client.clone(),
            cancel_token.clone(),
            config.clone(),
            stats.clone(),
            data.clone(),
            queue_config.clone(),
        ));
    }

    let mut signals = Signals::new(config.shutdown_signals.clone())?;

    select! {
        _ = cancel_token.cancelled() => {}
        sig = signals.next() => {
            println!("Received signal {:?}", sig);
            cancel_token.cancel();
        }
    }

    joinset.join_all().await;

    let stats = Arc::try_unwrap(stats)
        .expect("Failed to unwrap Arc - there are still references to stats")
        .into_inner();

    Ok(stats)
}

pub async fn enqueue<T, DT, ET>(
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

pub async fn enqueue_in<T, DT, ET>(
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
        storage::enqueue_in(redis, &envelope, delay).await
    } else {
        storage::enqueue(redis, &envelope).await
    }
}
