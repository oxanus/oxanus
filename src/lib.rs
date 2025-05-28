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
pub use crate::job_envelope::{Job, JobEnvelope, JobId};
pub use crate::queue::{Queue, QueueConfig, QueueKind, QueueThrottle};
use crate::result_collector::Stats;
pub use crate::worker::Worker;
pub use crate::worker_state::WorkerState;
pub use signal_hook::consts as signals;

pub async fn run<DT, ET>(
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

    tokio::spawn(retry_loop(config.clone(), cancel_token.clone()));
    tokio::spawn(schedule_loop(config.clone(), cancel_token.clone()));
    tokio::spawn(ping_loop(config.clone(), cancel_token.clone()));
    tokio::spawn(resurrect_loop(config.clone(), cancel_token.clone()));

    for queue_config in &config.queues {
        joinset.spawn(coordinator::run(
            config.clone(),
            cancel_token.clone(),
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

    tracing::info!("Shutting down");

    joinset.join_all().await;

    let stats = Arc::try_unwrap(stats)
        .expect("Failed to unwrap Arc - there are still references to stats")
        .into_inner();

    tracing::info!("Gracefully shut down");

    Ok(stats)
}

async fn retry_loop<DT, ET>(
    config: Arc<Config<DT, ET>>,
    cancel_token: CancellationToken,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config.storage.retry_loop(cancel_token).await
}

async fn schedule_loop<DT, ET>(
    config: Arc<Config<DT, ET>>,
    cancel_token: CancellationToken,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config.storage.schedule_loop(cancel_token).await
}

async fn ping_loop<DT, ET>(
    config: Arc<Config<DT, ET>>,
    cancel_token: CancellationToken,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config.storage.ping_loop(cancel_token).await
}

async fn resurrect_loop<DT, ET>(
    config: Arc<Config<DT, ET>>,
    cancel_token: CancellationToken,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    config.storage.resurrect_loop(cancel_token).await
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
