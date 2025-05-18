pub mod config;
pub mod error;
pub mod job_envelope;
pub mod queue;
pub mod worker;
pub mod worker_registry;
pub mod worker_state;

use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

pub use crate::config::Config;
pub use crate::error::OxanusError;
pub use crate::job_envelope::JobEnvelope;
pub use crate::queue::{Queue, QueueConfig, QueueKind, QueueRetry, QueueRetryBackoff};
pub use crate::worker::Worker;
pub use crate::worker_registry::WorkerRegistry;
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
    redis: &redis::Client,
    config: Config<DT, ET>,
    data: WorkerState<DT>,
) -> Result<Stats, OxanusError> {
    let config = Arc::new(config);
    let mut joinset = tokio::task::JoinSet::new();
    let stats = Arc::new(Mutex::new(Stats::default()));

    for queue_config in &config.queues {
        let redis = redis::aio::ConnectionManager::new(redis.clone()).await?;
        joinset.spawn(run_queue_workers(
            redis.clone(),
            config.clone(),
            stats.clone(),
            data.clone(),
            queue_config.clone(),
        ));
    }

    joinset.join_all().await;

    let stats = Arc::try_unwrap(stats)
        .expect("Failed to unwrap Arc - there are still references to stats")
        .into_inner()
        .expect("Failed to unwrap Mutex - it was poisoned");

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

    let envelope_str = serde_json::to_string(&envelope)?;

    if delay > 0 {
        let now = chrono::Utc::now().timestamp_micros();
        let _: i32 = redis
            .zadd(
                "oxanus:scheduled",
                envelope_str,
                now + 1_000_000 * delay as i64,
            )
            .await?;
    } else {
        let _: i32 = redis.rpush(queue_key, envelope_str).await?;
    }

    Ok(0)
}

async fn run_redis_listeners(
    redis: redis::aio::ConnectionManager,
    queue_config: QueueConfig,
    job_tx: mpsc::Sender<WorkerEvent>,
    semaphores: Arc<KeyedSemaphores>,
) {
    let mut all_queues = Vec::new();
    let mut redis = redis.clone();

    match queue_config.kind {
        QueueKind::Static { key } => all_queues.push(key),
        QueueKind::Dynamic { prefix } => {
            let queues: Vec<String> = redis.keys(format!("{}*", prefix)).await.unwrap();
            all_queues.extend(queues);
        }
    }

    for queue in all_queues {
        tokio::spawn(redis_listener(
            redis.clone(),
            queue,
            job_tx.clone(),
            semaphores.clone(),
        ));
    }
}

async fn redis_listener(
    redis: redis::aio::ConnectionManager,
    queue: String,
    job_tx: mpsc::Sender<WorkerEvent>,
    semaphores: Arc<KeyedSemaphores>,
) {
    let mut redis = redis.clone();

    loop {
        let semaphore = semaphores.get_or_create(queue.clone()).await;
        let permit = semaphore.acquire_owned().await.unwrap();
        let msg: redis::Value = redis
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

pub struct KeyedSemaphores {
    permits: usize,
    inner: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl KeyedSemaphores {
    pub fn new(permits: usize) -> Self {
        Self {
            permits,
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_or_create(&self, key: String) -> Arc<Semaphore> {
        let mut map = self.inner.lock().expect("Failed to lock semaphore map");
        Arc::clone(
            map.entry(key)
                .or_insert_with(|| Arc::new(Semaphore::new(self.permits))),
        )
    }
}

pub async fn run_queue_workers<
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    redis: redis::aio::ConnectionManager,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    data: WorkerState<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanusError> {
    let concurrency = queue_config.concurrency;
    let (result_tx, result_rx) = mpsc::channel::<Result<(), ET>>(concurrency);
    let (job_tx, mut job_rx) = mpsc::channel::<WorkerEvent>(concurrency);
    let semaphores = Arc::new(KeyedSemaphores::new(concurrency));

    tokio::spawn(collect_results(
        result_rx,
        config.clone(),
        job_tx.clone(),
        stats.clone(),
    ));
    run_redis_listeners(
        redis.clone(),
        queue_config.clone(),
        job_tx.clone(),
        semaphores.clone(),
    )
    .await;

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
            let redis = redis.clone();
            async move {
                let data = data.clone();
                let result = run_worker(redis.clone(), queue, job, envelope, data).await;
                drop(permit);
                result_tx
                    .send(result)
                    .await
                    .unwrap_or_else(|e| println!("Failed to send result: {}", e));
            }
        });
    }
}

pub type BoxedWorker<DT, ET> = Box<dyn Worker<Data = DT, Error = ET>>;

#[derive(Debug)]
enum WorkerEvent {
    Job {
        queue: String,
        job: serde_json::Value,
        permit: OwnedSemaphorePermit,
    },
    Exit,
}

async fn run_worker<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync>(
    redis: redis::aio::ConnectionManager,
    queue: String,
    job: BoxedWorker<DT, ET>,
    envelope: JobEnvelope,
    data: WorkerState<DT>,
) -> Result<(), ET> {
    // println!("Worker started");
    tracing::info!("Queue: {} - Worker started", queue);
    let mut redis = redis.clone();
    // println!("Processing job");
    let result = job.process(&data).await;
    // println!("Job processed");
    let is_err = result.is_err();
    let max_retries = job.max_retries();
    let retry_delay = job.retry_delay(envelope.meta.retries);

    let can_archive = if is_err {
        if envelope.meta.retries < max_retries {
            let updated_job = envelope.with_retries_incremented();
            let updated_job_str =
                serde_json::to_string(&updated_job).expect("Failed to serialize job");
            let _: i32 = redis
                .rpush(&queue, updated_job_str.clone())
                .await
                .expect("Failed to send job to queue");

            let now = chrono::Utc::now().timestamp_micros();
            let _: Option<i64> = redis
                .zadd(
                    "oxanus:retries",
                    updated_job_str,
                    now + 1_000_000 * retry_delay as i64,
                )
                .await
                .expect("Failed to send job to queue");

            true
        } else {
            println!("Job {} failed after {} retries", envelope.uuid, max_retries);
            true
        }
    } else {
        true
    };

    if can_archive {
        // TODO: for redis
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
        let processed = if let Ok(mut stats) = stats.lock() {
            stats.processed += 1;
            match result {
                Ok(_) => stats.succeeded += 1,
                Err(_e) => stats.failed += 1,
            }

            stats.processed
        } else {
            0
        };

        if let Some(exit_when_finished) = config.exit_when_finished {
            if processed >= exit_when_finished {
                job_tx.send(WorkerEvent::Exit).await.unwrap();
            }
        }
    }
}
