use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::Config;
use crate::error::OxanusError;
use crate::queue::{QueueConfig, QueueThrottle};
use crate::semaphores_map::SemaphoresMap;
use crate::storage::Storage;
use crate::throttler::Throttler;
use crate::worker_event::WorkerJob;

pub async fn run<DT, ET>(
    config: Arc<Config<DT, ET>>,
    queue_config: QueueConfig,
    queue_key: String,
    job_tx: mpsc::Sender<WorkerJob>,
    semaphores: Arc<SemaphoresMap>,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    loop {
        let semaphore = semaphores.get_or_create(queue_key.clone()).await;
        let permit = semaphore.acquire_owned().await.unwrap();

        tokio::select! {
            result = pop_queue_message(&config.storage, &queue_config, &queue_key) => {
                let job_id = result.expect("Failed to pop queue message");
                let job = WorkerJob { job_id, permit };
                job_tx
                    .send(job)
                    .await
                    .expect("Failed to send job to worker");
            }
            _ = config.cancel_token.cancelled() => {
                tracing::debug!("Stopping dispatcher for queue {}", queue_key);
                drop(permit);
                break;
            }
        }
    }
}

async fn pop_queue_message(
    storage: &Storage,
    queue_config: &QueueConfig,
    queue_key: &str,
) -> Result<String, OxanusError> {
    match &queue_config.throttle {
        Some(throttle) => pop_queue_message_w_throttle(storage, queue_key, throttle).await,
        None => pop_queue_message_wo_throttle(storage, queue_key, 10.0).await,
    }
}

async fn pop_queue_message_wo_throttle(
    storage: &Storage,
    queue_key: &str,
    timeout: f64,
) -> Result<String, OxanusError> {
    loop {
        if let Some(job_id) = storage.blocking_dequeue(queue_key, timeout).await? {
            return Ok(job_id);
        }
    }
}

async fn pop_queue_message_w_throttle(
    storage: &Storage,
    queue_key: &str,
    throttle: &QueueThrottle,
) -> Result<String, OxanusError> {
    let mut redis_manager = storage.redis_manager().await?;
    loop {
        let throttler = Throttler::new(
            redis_manager.clone(),
            queue_key,
            throttle.limit,
            throttle.window_ms,
        );

        let state = throttler.state(&mut redis_manager).await?;

        if state.is_allowed {
            if let Some(job_id) = storage.dequeue(queue_key).await? {
                throttler.consume().await?;
                return Ok(job_id);
            }
        }

        sleep(Duration::from_millis(state.throttled_for.unwrap_or(100))).await;
    }
}
