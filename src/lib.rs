mod cemetery;
pub mod config;
pub mod error;
pub mod job_envelope;
pub mod queue;
mod redis_helper;
mod retrying;
mod scheduling;
mod semaphores_map;
mod timed_migrator;
pub mod worker;
mod worker_event;
mod worker_registry;
pub mod worker_state;

use redis::AsyncCommands;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

pub use crate::config::Config;
pub use crate::error::OxanusError;
pub use crate::job_envelope::JobEnvelope;
pub use crate::queue::{Queue, QueueConfig, QueueKind, QueueRetry, QueueRetryBackoff};
use crate::semaphores_map::SemaphoresMap;
use crate::worker::BoxedWorker;
pub use crate::worker::Worker;
use crate::worker_event::WorkerEvent;
pub use crate::worker_state::WorkerState;

#[derive(Default, Debug)]
pub struct Stats {
    pub processed: u64,
    pub succeeded: u64,
    pub failed: u64,
}

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
        joinset.spawn(run_queue_workers(
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

async fn run_redis_listeners(
    redis_client: redis::Client,
    queue_config: QueueConfig,
    job_tx: mpsc::Sender<WorkerEvent>,
    semaphores: Arc<SemaphoresMap>,
) {
    let mut tracked_queues = HashSet::new();

    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client.clone())
        .await
        .unwrap();

    loop {
        let all_queues: HashSet<String> = match &queue_config.kind {
            QueueKind::Static { key } => HashSet::from([key.clone()]),
            QueueKind::Dynamic { prefix } => {
                redis_manager.keys(format!("{}*", prefix)).await.unwrap()
            }
        };
        let new_queues: HashSet<String> = all_queues.difference(&tracked_queues).cloned().collect();

        for queue in new_queues {
            tracing::info!("Tracking queue: {}", queue);

            tokio::spawn(redis_listener(
                redis_client.clone(),
                queue.clone(),
                job_tx.clone(),
                semaphores.clone(),
            ));

            tracked_queues.insert(queue);
        }

        if queue_config.kind.is_dynamic() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        } else {
            break;
        }
    }
}

async fn redis_listener(
    redis_client: redis::Client,
    queue: String,
    job_tx: mpsc::Sender<WorkerEvent>,
    semaphores: Arc<SemaphoresMap>,
) {
    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client.clone())
        .await
        .unwrap();

    loop {
        let semaphore = semaphores.get_or_create(queue.clone()).await;
        let permit = semaphore.acquire_owned().await.unwrap();
        let msg: redis::Value = redis_manager
            .blpop(&queue, 10.0)
            .await
            .expect("Failed to read job from queue");

        let value: Option<(String, String)> =
            redis::FromRedisValue::from_redis_value(&msg).expect("Failed to parse job");

        let (_q, msg) = match value {
            Some(value) => value,
            None => {
                continue;
            }
        };

        match serde_json::from_str(&msg) {
            Ok(msg) => {
                let job = WorkerEvent::Job {
                    queue: _q,
                    job: msg,
                    permit,
                };
                job_tx
                    .send(job)
                    .await
                    .expect("Failed to send job to worker");
            }
            Err(e) => {
                println!("Failed to parse job: {}", e);
            }
        }
    }
}

pub async fn run_queue_workers<
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    redis_client: redis::Client,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    data: WorkerState<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanusError> {
    let concurrency = queue_config.concurrency;
    let (result_tx, result_rx) = mpsc::channel::<Result<(), ET>>(concurrency);
    let (job_tx, mut job_rx) = mpsc::channel::<WorkerEvent>(concurrency);
    let semaphores = Arc::new(SemaphoresMap::new(concurrency));

    tokio::spawn(collect_results(
        result_rx,
        config.clone(),
        job_tx.clone(),
        stats.clone(),
    ));
    tokio::spawn(run_redis_listeners(
        redis_client.clone(),
        queue_config.clone(),
        job_tx.clone(),
        semaphores.clone(),
    ));

    let redis_manager = redis::aio::ConnectionManager::new(redis_client.clone())
        .await
        .unwrap();

    loop {
        let (queue, job_value, permit) = match job_rx.recv().await {
            Some(job) => match job {
                WorkerEvent::Job { queue, job, permit } => (queue, job, permit),
                WorkerEvent::Exit => {
                    return Ok(());
                }
            },
            None => {
                continue;
            }
        };

        let envelope: JobEnvelope = match serde_json::from_value(job_value) {
            Ok(envelope) => envelope,
            Err(e) => {
                println!("Failed to parse job envelope: {}", e);
                continue;
            }
        };
        tracing::debug!("Received envelope: {:?}", &envelope);
        let job = match config.registry.build(&envelope.job, envelope.args.clone()) {
            Ok(job) => job,
            Err(e) => {
                println!("Invalid job: {} - {}", &envelope.job, e);
                continue;
            }
        };

        tokio::spawn({
            let data = data.clone();
            let result_tx = result_tx.clone();
            let redis_manager = redis_manager.clone();
            async move {
                let data = data.clone();
                let result = run_worker(redis_manager.clone(), queue, job, envelope, data).await;
                drop(permit);
                result_tx
                    .send(result)
                    .await
                    .unwrap_or_else(|e| println!("Failed to send result: {}", e));
            }
        });
    }
}

async fn run_worker<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync>(
    redis: redis::aio::ConnectionManager,
    queue: String,
    job: BoxedWorker<DT, ET>,
    envelope: JobEnvelope,
    data: WorkerState<DT>,
) -> Result<(), ET> {
    tracing::info!("Queue: {} - Worker started", queue);

    let result = job.process(&data).await;
    let is_err = result.is_err();
    let max_retries = job.max_retries();
    let retry_delay = job.retry_delay(envelope.meta.retries);

    if is_err {
        if envelope.meta.retries < max_retries {
            let updated_envelope = envelope.with_retries_incremented();

            retrying::retry_in(&redis, updated_envelope, retry_delay)
                .await
                .expect("Failed to retry job");
        } else {
            tracing::error!("Job {} failed after {} retries", envelope.uuid, max_retries);
            cemetery::add(&redis, envelope)
                .await
                .expect("Failed to add job to cemetery");
        }
    }

    result
}

async fn collect_results<
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    mut rx: mpsc::Receiver<Result<(), ET>>,
    config: Arc<Config<DT, ET>>,
    job_tx: mpsc::Sender<WorkerEvent>,
    stats: Arc<Mutex<Stats>>,
) {
    while let Some(result) = rx.recv().await {
        let processed = {
            let mut stats = stats.lock().await;
            stats.processed += 1;
            match result {
                Ok(_) => stats.succeeded += 1,
                Err(_e) => stats.failed += 1,
            }

            stats.processed
        };

        if let Some(exit_when_finished) = config.exit_when_finished {
            if processed >= exit_when_finished {
                job_tx.send(WorkerEvent::Exit).await.unwrap();
            }
        }
    }
}
