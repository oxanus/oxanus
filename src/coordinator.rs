use redis::AsyncCommands;
use std::collections::HashSet;
use std::num::NonZero;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::time::sleep;

use crate::config::Config;
use crate::error::OxanusError;
use crate::executor;
use crate::job_envelope::JobEnvelope;
use crate::queue::{QueueConfig, QueueKind, QueueThrottle};
use crate::semaphores_map::SemaphoresMap;
use crate::throttler::Throttler;
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
            let job_name = envelope.job.clone();
            async move {
                let data = data.clone();
                let result =
                    executor::run(redis_manager.clone(), queue, job_name, job, envelope, data)
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
            tracing::info!(
                queue = queue,
                config.throttle = format!("{:?}", queue_config.throttle),
                "Tracking queue"
            );

            tokio::spawn(redis_listener(
                redis_client.clone(),
                queue_config.clone(),
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

async fn pop_queue_message(
    redis_manager: &mut redis::aio::ConnectionManager,
    queue_config: &QueueConfig,
    queue_key: &str,
) -> Result<String, OxanusError> {
    match &queue_config.throttle {
        Some(throttle) => pop_queue_message_w_throttle(redis_manager, queue_key, throttle).await,
        None => pop_queue_message_wo_throttle(redis_manager, queue_key).await,
    }
}

async fn pop_queue_message_wo_throttle(
    redis_manager: &mut redis::aio::ConnectionManager,
    queue_key: &str,
) -> Result<String, OxanusError> {
    loop {
        let msg: redis::Value = redis_manager.blpop(queue_key, 10.0).await?;
        let value: Option<(String, String)> = redis::FromRedisValue::from_redis_value(&msg)?;

        if let Some((_, msg)) = value {
            return Ok(msg);
        }
    }
}

async fn pop_queue_message_w_throttle(
    redis_manager: &mut redis::aio::ConnectionManager,
    queue_key: &str,
    throttle: &QueueThrottle,
) -> Result<String, OxanusError> {
    loop {
        let throttler = Throttler::new(
            redis_manager.clone(),
            queue_key,
            throttle.limit,
            throttle.window_ms,
        );

        let state = throttler.state(redis_manager).await?;

        if state.is_allowed {
            let msg: redis::Value = redis_manager
                .lpop(&queue_key, Some(NonZero::new(1).unwrap()))
                .await?;
            let value: Vec<String> = redis::FromRedisValue::from_redis_value(&msg)?;
            if let Some(msg) = value.first() {
                throttler.consume().await?;
                return Ok(msg.clone());
            }
        }

        sleep(Duration::from_millis(state.throttled_for.unwrap_or(100))).await;
    }
}

async fn redis_listener(
    redis_client: redis::Client,
    queue_config: QueueConfig,
    queue_key: String,
    job_tx: mpsc::Sender<WorkerEvent>,
    semaphores: Arc<SemaphoresMap>,
) {
    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client.clone())
        .await
        .unwrap();

    loop {
        let semaphore = semaphores.get_or_create(queue_key.clone()).await;
        let permit = semaphore.acquire_owned().await.unwrap();

        let msg = pop_queue_message(&mut redis_manager, &queue_config, &queue_key)
            .await
            .expect("Failed to pop queue message");

        match serde_json::from_str(&msg) {
            Ok(msg) => {
                let job = WorkerEvent::Job {
                    queue: queue_key.clone(),
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
