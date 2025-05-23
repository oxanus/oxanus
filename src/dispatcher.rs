use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::error::OxanusError;
use crate::queue::{QueueConfig, QueueThrottle};
use crate::semaphores_map::SemaphoresMap;
use crate::storage;
use crate::throttler::Throttler;
use crate::worker_event::{WorkerEvent, WorkerEventJob};

pub async fn run(
    redis_client: redis::Client,
    cancel_token: CancellationToken,
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

        select! {
            result = pop_queue_message(&mut redis_manager, &queue_config, &queue_key) => {
                let job_id = result.expect("Failed to pop queue message");
                let job = WorkerEvent::Job(WorkerEventJob { job_id, permit });
                job_tx
                    .send(job)
                    .await
                    .expect("Failed to send job to worker");
            }
            _ = cancel_token.cancelled() => {
                tracing::debug!("Stopping dispatcher for queue {}", queue_key);
                drop(permit);
                break;
            }
        }

        // let job_id = pop_queue_message(&mut redis_manager, &queue_config, &queue_key)
        //     .await
        //     .expect("Failed to pop queue message");

        // let job = WorkerEvent::Job(WorkerEventJob { job_id, permit });
        // job_tx
        //     .send(job)
        //     .await
        //     .expect("Failed to send job to worker");
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
        if let Some(job_id) = storage::blocking_dequeue(redis_manager, queue_key, 10.0).await? {
            return Ok(job_id);
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
            if let Some(job_id) = storage::dequeue(redis_manager, queue_key).await? {
                throttler.consume().await?;
                return Ok(job_id);
            }
        }

        sleep(Duration::from_millis(state.throttled_for.unwrap_or(100))).await;
    }
}
