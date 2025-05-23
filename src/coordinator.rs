use redis::AsyncCommands;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::select;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::error::OxanusError;
use crate::job_envelope::JobEnvelope;
use crate::queue::{QueueConfig, QueueKind};
use crate::semaphores_map::SemaphoresMap;
use crate::worker_event::{WorkerEvent, WorkerEventJob};
use crate::worker_state::WorkerState;
use crate::{
    dispatcher, executor,
    result_collector::{self, Stats},
    storage,
};

pub async fn run<
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
>(
    redis_client: redis::Client,
    cancel_token: CancellationToken,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    data: WorkerState<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanusError> {
    let concurrency = queue_config.concurrency;
    let (result_tx, result_rx) = mpsc::channel::<Result<(), ET>>(concurrency);
    let (job_tx, mut job_rx) = mpsc::channel::<WorkerEvent>(concurrency);
    let semaphores = Arc::new(SemaphoresMap::new(concurrency));

    tokio::spawn(result_collector::run(
        result_rx,
        cancel_token.clone(),
        config.clone(),
        stats.clone(),
    ));
    tokio::spawn(run_queue_watcher(
        redis_client.clone(),
        cancel_token.clone(),
        queue_config.clone(),
        job_tx.clone(),
        semaphores.clone(),
    ));

    let redis_manager = redis::aio::ConnectionManager::new(redis_client.clone())
        .await
        .unwrap();

    loop {
        select! {
            job = job_rx.recv() => {
                if let Some(job) = job {
                    match job {
                        WorkerEvent::Job(job_event) => {
                            process_job(
                                redis_manager.clone(),
                                config.clone(),
                                data.clone(),
                                result_tx.clone(),
                                job_event,
                            )
                            .await;
                        }
                    };
                }
            }
            _ = cancel_token.cancelled() => {
                break;
            }
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    loop {
        let busy_count = semaphores.busy_count().await;
        if busy_count == 0 {
            break;
        }

        tracing::info!("Waiting for {} worker to finish", busy_count);
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }

    Ok(())
}

async fn process_job<DT, ET>(
    redis_manager: redis::aio::ConnectionManager,
    config: Arc<Config<DT, ET>>,
    data: WorkerState<DT>,
    result_tx: mpsc::Sender<Result<(), ET>>,
    job_event: WorkerEventJob,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let envelope: JobEnvelope = match storage::get(&redis_manager, &job_event.job_id).await {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            tracing::warn!("Job {} not found", job_event.job_id);
            return;
        }
        Err(e) => {
            println!("Failed to get job envelope: {}", e);
            return;
        }
    };

    tracing::debug!("Received envelope: {:?}", &envelope);
    let job = match config
        .registry
        .build(&envelope.job.name, envelope.job.args.clone())
    {
        Ok(job) => job,
        Err(e) => {
            println!("Invalid job: {} - {}", &envelope.job.name, e);
            return;
        }
    };

    tokio::spawn({
        let data = data.clone();
        let result_tx = result_tx.clone();
        let redis_manager = redis_manager.clone();
        let job_name = envelope.job.name.clone();
        async move {
            let result = executor::run(redis_manager.clone(), job_name, job, envelope, data).await;
            drop(job_event.permit);
            result_tx.send(result).await.ok();
        }
    });
}

async fn run_queue_watcher(
    redis_client: redis::Client,
    cancel_token: CancellationToken,
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

            tokio::spawn(dispatcher::run(
                redis_client.clone(),
                cancel_token.clone(),
                queue_config.clone(),
                queue.clone(),
                job_tx.clone(),
                semaphores.clone(),
            ));

            tracked_queues.insert(queue);
        }

        if cancel_token.is_cancelled() {
            break;
        } else if queue_config.kind.is_dynamic() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        } else {
            break;
        }
    }
}
