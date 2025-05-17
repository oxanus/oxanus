pub mod config;
pub mod error;
pub mod job_envelope;
pub mod processor;
pub mod queue;
pub mod worker;
pub mod worker_registry;
pub mod worker_state;

use processor::ProcessorQueue;
use redis::AsyncCommands;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

pub use crate::config::Config;
pub use crate::error::OxanusError;
pub use crate::job_envelope::JobEnvelope;
pub use crate::processor::Processor;
pub use crate::queue::{QueueDynamic, QueueStatic};
pub use crate::worker::Worker;
pub use crate::worker_registry::WorkerRegistry;
pub use crate::worker_state::WorkerState;

#[derive(Default, Debug)]
pub struct Stats {
    pub processed: u64,
    pub succeeded: u64,
    pub failed: u64,
}

pub async fn redis() -> Result<redis::aio::ConnectionManager, redis::RedisError> {
    redis::aio::ConnectionManager::new(
        redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL is not set")).unwrap(),
    )
    .await
}

pub async fn run<
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    // pool: &Pool<Postgres>,
    redis: &redis::aio::ConnectionManager,
    config: Config<DT, ET>,
    data: WorkerState<DT>,
) -> Result<Stats, OxanusError> {
    // let redis = redis().await?;
    // let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;
    // let queue_configs: Vec<QueueConfig> = config.queues.values().map(|q| q.config()).collect();
    let config = Arc::new(config);
    let mut joinset = tokio::task::JoinSet::new();
    let stats = Arc::new(Mutex::new(Stats::default()));

    for processor in &config.processors {
        joinset.spawn(run_queue_workers(
            // pgmq.clone(),
            redis.clone(),
            config.clone(),
            stats.clone(),
            data.clone(),
            processor.clone(),
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
    _redis: &redis::aio::ConnectionManager,
    _config: &Config<DT, ET>,
) -> Result<(), OxanusError> {
    // let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;

    // for queue in config.queues.values() {
    //     pgmq.create(queue.name()).await.ok();
    // }

    Ok(())
}

pub async fn enqueue<
    T,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    redis: &redis::aio::ConnectionManager,
    queue: &QueueStatic,
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
    queue: &QueueStatic,
    job: T,
    delay: u64,
) -> Result<i64, OxanusError>
where
    T: Worker<Data = DT, Error = ET> + serde::Serialize,
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let mut redis = redis.clone();

    // let pgmq = pgmq::PGMQueue::new_with_pool(pool.clone()).await;
    let envelope = JobEnvelope::new(queue.name.clone(), job)?;
    // let msg_id = pgmq
    //     .send_delay(queue.name(), &serde_json::to_value(&envelope)?, delay)
    //     .await?;

    if delay > 0 {
        let now = chrono::Utc::now().timestamp_micros();
        let _: i32 = redis
            .zadd(
                "oxanus:scheduled",
                queue.name.clone(),
                now + 1_000_000 * delay as i64,
            )
            .await?;
    } else {
        let _: i32 = redis
            .rpush(queue.name.clone(), serde_json::to_string(&envelope)?)
            .await?;
    }

    Ok(0)
}

async fn redis_listener(
    redis: redis::aio::ConnectionManager,
    queues: Vec<ProcessorQueue>,
    job_tx: mpsc::Sender<WorkerEvent>,
    worker_capacity: Arc<Semaphore>,
) {
    let mut all_queues = Vec::new();
    let mut redis = redis.clone();

    for queue in queues {
        match queue {
            ProcessorQueue::Queue(queue) => all_queues.push(queue),
            ProcessorQueue::Pattern(pattern) => {
                let queues: Vec<String> = redis.keys(pattern).await.unwrap();
                all_queues.extend(queues);
            }
        }
    }

    // dbg!(&all_queues);

    // for queue in all_queues {
    //     let queue = queue.clone();
    //     tokio::spawn(redis_listener_worker(redis.clone(), queue, job_tx.clone()));
    // }

    loop {
        let permit = worker_capacity.clone().acquire_owned().await.unwrap();
        let msg: redis::Value = redis
            .blpop(&all_queues, 10.0)
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
                job_tx
                    .send(WorkerEvent::Job {
                        queue: _q,
                        job: msg,
                        permit,
                    })
                    .await
                    .unwrap();
            }
            Err(e) => {
                println!("Failed to parse job: {}", e);
            }
        }
    }
}

async fn redis_listener_worker(
    redis: redis::aio::ConnectionManager,
    queue: String,
    job_tx: mpsc::Sender<serde_json::Value>,
) {
    tracing::info!("Redis listener worker started for queue: {}", queue);
    let mut redis = redis.clone();
    loop {
        let msg: redis::Value = redis
            .blpop(&queue, 10.0)
            .await
            .expect("Failed to read job from queue");

        dbg!(&msg);
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
                job_tx.send(msg).await.unwrap();
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
    // pgmq: pgmq::PGMQueue,
    redis: redis::aio::ConnectionManager,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    data: WorkerState<DT>,
    processor: Processor,
) -> Result<(), OxanusError> {
    let concurrency = processor.concurrency;
    let (result_tx, result_rx) = mpsc::channel::<Result<(), ET>>(concurrency);
    let (job_tx, mut job_rx) = mpsc::channel::<WorkerEvent>(concurrency);
    let worker_capacity = Arc::new(Semaphore::new(concurrency));

    tokio::spawn(collect_results(
        result_rx,
        config.clone(),
        job_tx.clone(),
        stats.clone(),
    ));
    tokio::spawn(redis_listener(
        redis.clone(),
        processor.queues.clone(),
        job_tx.clone(),
        worker_capacity.clone(),
    ));

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
            // let pgmq = pgmq.clone();
            let data = data.clone();
            let result_tx = result_tx.clone();
            let redis = redis.clone();
            async move {
                let data = data.clone();
                let result = run_worker(
                    // pgmq,
                    redis.clone(),
                    "queue_tbd".to_string(),
                    job,
                    envelope,
                    data,
                )
                .await;
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

enum WorkerEvent {
    Job {
        queue: String,
        job: serde_json::Value,
        permit: OwnedSemaphorePermit,
    },
    Exit,
}

async fn run_worker<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync>(
    // pgmq: pgmq::PGMQueue,
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

            // dbg!(&stats);
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
