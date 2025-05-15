pub mod error;
pub mod job_envelope;
pub mod queue;
pub mod worker;
pub mod worker_registry;
pub mod worker_state;

use queue::QueueConfig;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};
use std::sync::Mutex;

pub use crate::error::OxanusError;
pub use crate::job_envelope::JobEnvelope;
pub use crate::queue::Queue;
pub use crate::worker::Worker;
pub use crate::worker_registry::WorkerRegistry;
pub use crate::worker_state::WorkerState;

pub struct Config<DT, ET> {
    registry: WorkerRegistry<DT, ET>,
    queues: HashMap<String, Queue>,
    exit_when_idle: bool,
}

impl<DT, ET> Config<DT, ET> {
    pub fn new() -> Self {
        Self {
            registry: WorkerRegistry::new(),
            queues: HashMap::new(),
            exit_when_idle: false,
        }
    }

    pub fn register_queue(mut self, queue: Queue) -> Self {
        self.queues.insert(queue.name().to_string(), queue);
        self
    }

    pub fn register_worker<T>(mut self) -> Self
    where
        T: Worker<Data = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
    {
        self.registry.register::<T>();
        self
    }

    pub fn exit_when_idle(mut self) -> Self {
        self.exit_when_idle = true;
        self
    }
}

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
    pool: &Pool<Postgres>,
    config: Config<DT, ET>,
    data: WorkerState<DT>,
) -> Result<Stats, OxanusError> {
    let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;
    let queue_configs: Vec<QueueConfig> = config.queues.values().map(|q| q.config()).collect();
    let config = Arc::new(config);
    let mut joinset = tokio::task::JoinSet::new();
    let stats = Arc::new(Mutex::new(Stats::default()));

    for queue_config in queue_configs {
        joinset.spawn(run_queue_workers(
            pgmq.clone(),
            config.clone(),
            stats.clone(),
            data.clone(),
            queue_config,
        ));
    }

    joinset.join_all().await;

    let stats = Arc::try_unwrap(stats)
        .expect("Failed to unwrap Arc - there are still references to stats")
        .into_inner()
        .expect("Failed to unwrap Mutex - it was poisoned");

    Ok(stats)
}

pub async fn setup<
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    pool: &Pool<Postgres>,
    config: &Config<DT, ET>,
) -> Result<(), OxanusError> {
    let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;

    for queue in config.queues.values() {
        pgmq.create(queue.name()).await.ok();
    }

    Ok(())
}

pub async fn enqueue<
    T,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    pool: &Pool<Postgres>,
    queue: &Queue,
    job: T,
) -> Result<i64, OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    enqueue_in(pool, queue, job, 0).await
}

pub async fn enqueue_in<
    T,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    pool: &Pool<Postgres>,
    queue: &Queue,
    job: T,
    delay: u64,
) -> Result<i64, OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;
    let envelope = JobEnvelope::new(job)?;
    let msg_id = pgmq
        .send_delay(queue.name(), &serde_json::to_value(&envelope)?, delay)
        .await?;

    Ok(msg_id)
}

pub async fn run_queue_workers<
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    pgmq: pgmq::PGMQueue,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    data: WorkerState<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanusError> {
    tracing::info!(
        "Queue: {} (concurrency: {})",
        queue_config.name,
        queue_config.concurrency
    );

    pgmq.create(&queue_config.name).await.ok();

    let concurrency = queue_config.concurrency;
    let (result_tx, result_rx) = mpsc::channel::<Result<(), ET>>(concurrency);
    let worker_capacity = Arc::new(Semaphore::new(concurrency));

    tokio::spawn(collect_results(result_rx, stats.clone()));

    loop {
        let permit = worker_capacity.clone().acquire_owned().await.unwrap();
        let msg: Option<pgmq::Message<serde_json::Value>> =
            match pgmq.read(&queue_config.name, Some(30)).await {
                Ok(msg) => msg,
                Err(e) => {
                    println!("Failed to read job from queue: {}", e);
                    continue;
                }
            };
        if let Some(job_msg) = msg {
            let envelope: JobEnvelope = match serde_json::from_value(job_msg.message) {
                Ok(envelope) => envelope,
                Err(e) => {
                    if let Err(e) = pgmq.archive(&queue_config.name, job_msg.msg_id).await {
                        println!("Failed to archive job {}: {}", job_msg.msg_id, e);
                    }
                    println!("Failed to parse job envelope: {}", e);
                    continue;
                }
            };
            tracing::debug!("Received envelope: {:?}", &envelope);
            let job = match config.registry.build(&envelope.job, envelope.args.clone()) {
                Ok(job) => job,
                Err(e) => {
                    println!("Invalid job: {} - {}", &envelope.job, e);
                    if let Err(e) = pgmq.archive(&queue_config.name, job_msg.msg_id).await {
                        println!("Failed to archive job {}: {}", job_msg.msg_id, e);
                    }
                    continue;
                }
            };

            tokio::spawn({
                let pgmq = pgmq.clone();
                let queue = queue_config.name.clone();
                let data = data.clone();
                let result_tx = result_tx.clone();
                async move {
                    let data = data.clone();
                    let result = run_worker(
                        pgmq,
                        queue,
                        WorkerEvent::Job(job_msg.msg_id, job, envelope),
                        data,
                    )
                    .await;
                    result_tx
                        .send(result)
                        .await
                        .unwrap_or_else(|e| println!("Failed to send result: {}", e));
                    drop(permit);
                }
            });
        } else {
            drop(permit);
            if config.exit_when_idle && worker_capacity.available_permits() == concurrency {
                return Ok(());
            }
            // println!("No job found, sleeping");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
}

enum WorkerEvent<DT, ET> {
    Job(i64, Box<dyn Worker<Data = DT, Error = ET>>, JobEnvelope),
}

async fn run_worker<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync>(
    pgmq: pgmq::PGMQueue,
    queue: String,
    job: WorkerEvent<DT, ET>,
    data: WorkerState<DT>,
) -> Result<(), ET> {
    tracing::info!("Queue: {} - Worker started", queue);
    match job {
        WorkerEvent::Job(msg_id, job, envelope) => {
            let result = job.process(&data).await;
            let is_err = result.is_err();
            let max_retries = job.max_retries();
            let retry_delay = job.retry_delay(envelope.meta.retries);

            let can_archive = if is_err {
                if envelope.meta.retries < max_retries {
                    if let Err(e) = pgmq
                        .send_delay(&queue, &envelope.with_retries_incremented(), retry_delay)
                        .await
                    {
                        println!("Failed to send job to queue: {}", e);
                        false
                    } else {
                        true
                    }
                } else {
                    println!("Job {} failed after {} retries", msg_id, max_retries);
                    true
                }
            } else {
                true
            };

            if can_archive {
                if let Err(e) = pgmq.archive(&queue, msg_id).await {
                    println!("Failed to archive job {}: {}", msg_id, e);
                }
            }
            return result;
        }
    }
}

async fn collect_results<ET: std::error::Error + Send + Sync>(
    mut rx: mpsc::Receiver<Result<(), ET>>,
    stats: Arc<Mutex<Stats>>,
) {
    while let Some(result) = rx.recv().await {
        if let Ok(mut stats) = stats.lock() {
            stats.processed += 1;
            match result {
                Ok(_) => stats.succeeded += 1,
                Err(_e) => stats.failed += 1,
            }
        }
    }
}
