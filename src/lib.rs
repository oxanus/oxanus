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
use tokio::sync::{Mutex, Semaphore, mpsc};

pub use crate::error::OxanusError;
pub use crate::job_envelope::JobEnvelope;
pub use crate::queue::Queue;
pub use crate::worker::Worker;
pub use crate::worker_registry::WorkerRegistry;
pub use crate::worker_state::WorkerState;

pub struct Config<DT, ET> {
    registry: WorkerRegistry<DT, ET>,
    queues: HashMap<String, Box<dyn Queue>>,
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

    pub fn register_queue(mut self, queue: Box<dyn Queue>) -> Self {
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

    pub fn exit_when_done(mut self) -> Self {
        self.exit_when_idle = true;
        self
    }
}

pub async fn run<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync + 'static>(
    pool: &Pool<Postgres>,
    config: Config<DT, ET>,
    data: WorkerState<DT>,
) -> Result<(), OxanusError> {
    let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;
    let queue_configs: Vec<QueueConfig> = config.queues.values().map(|q| q.config()).collect();
    let config = Arc::new(config);
    let mut handles = Vec::new();

    for queue_config in queue_configs {
        handles.push(tokio::spawn(run_queue_workers(
            pgmq.clone(),
            config.clone(),
            data.clone(),
            queue_config,
        )));
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

pub async fn setup<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync + 'static>(
    pool: &Pool<Postgres>,
    config: &Config<DT, ET>,
) -> Result<(), OxanusError> {
    let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;

    for queue in config.queues.values() {
        pgmq.create(queue.name()).await.ok();
    }

    Ok(())
}

pub async fn enqueue<T, DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync + 'static>(
    pool: &Pool<Postgres>,
    job: T,
) -> Result<i64, OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    enqueue_in(pool, job, 0).await
}

pub async fn enqueue_in<T, DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync + 'static>(
    pool: &Pool<Postgres>,
    job: T,
    delay: u64,
) -> Result<i64, OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;
    let queue = job.queue();
    let envelope = JobEnvelope::new(job)?;
    let msg_id = pgmq
        .send_delay(queue.name(), &serde_json::to_value(&envelope)?, delay)
        .await?;

    Ok(msg_id)
}

pub async fn run_queue_workers<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync + 'static>(
    pgmq: pgmq::PGMQueue,
    config: Arc<Config<DT, ET>>,
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
    let (job_tx, job_rx) = mpsc::channel::<WorkerEvent<DT, ET>>(concurrency);
    let (result_tx, result_rx) = mpsc::channel::<Result<(), ET>>(concurrency);
    let job_rx = Arc::new(Mutex::new(job_rx));
    let worker_capacity = Arc::new(Semaphore::new(concurrency));

    for i in 0..concurrency {
        let job_rx = Arc::clone(&job_rx);
        let result_tx = result_tx.clone();
        let pgmq = pgmq.clone();
        let queue = queue_config.name.clone();
        let worker_capacity = Arc::clone(&worker_capacity);
        tokio::spawn(run_worker(
            i,
            pgmq,
            queue,
            job_rx,
            result_tx,
            worker_capacity,
            data.clone(),
        ));
    }

    drop(result_tx);

    tokio::spawn(collect_results(result_rx));

    loop {
        if worker_capacity.available_permits() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        }

        let msg: Option<pgmq::Message<serde_json::Value>> = match pgmq.read(&queue_config.name, Some(30)).await {
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
            if let Err(e) = job_tx.send(WorkerEvent::Job(job_msg.msg_id, job, envelope)).await {
                println!("Failed to send job to worker: {}", e);
            }
        } else {
            if config.exit_when_idle && worker_capacity.available_permits() == concurrency {
                for _ in 0..concurrency {
                    if let Err(e) = job_tx.send(WorkerEvent::Exit).await {
                        println!("Failed to send job to worker: {}", e);
                    }
                }
                return Ok(());
            }
            // println!("No job found, sleeping");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
}

enum WorkerEvent<DT, ET> {
    Exit,
    Job(i64, Box<dyn Worker<Data = DT, Error = ET>>, JobEnvelope),
}

async fn run_worker<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync>(
    id: usize,
    pgmq: pgmq::PGMQueue,
    queue: String,
    job_rx: Arc<Mutex<mpsc::Receiver<WorkerEvent<DT, ET>>>>,
    result_tx: mpsc::Sender<Result<(), ET>>,
    worker_capacity: Arc<Semaphore>,
    data: WorkerState<DT>,
) -> Result<(), OxanusError> {
    tracing::info!("Queue: {} - Worker {} started", queue, id);

    loop {
        let worker_capacity = worker_capacity.clone();
        let job_opt = {
            let mut locked_rx = job_rx.lock().await;
            locked_rx.recv().await
        };

        match job_opt {
            Some(WorkerEvent::Job(msg_id, job, envelope)) => {
                let permit = worker_capacity.acquire_owned().await?;
                let result = job.process(&data).await;
                let is_err = result.is_err();
                let max_retries = job.max_retries();
                let retry_delay = job.retry_delay(envelope.meta.retries);
                let _ = result_tx.send(result).await;

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

                drop(permit);
            }
            Some(WorkerEvent::Exit) | None => {
                println!("Worker {} exiting", id);
                return Ok(());
            }
        }
    }
}

async fn collect_results<ET: std::error::Error + Send + Sync>(mut rx: mpsc::Receiver<Result<(), ET>>) {
    while let Some(result) = rx.recv().await {
        match result {
            Ok(_) => (),
            Err(_e) => (),
        }
    }
}
