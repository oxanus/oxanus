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
use crate::worker_event::WorkerJob;
use crate::worker_state::WorkerState;
use crate::{
    dispatcher, executor,
    result_collector::{self, Stats},
};

pub async fn run<DT, ET>(
    config: Arc<Config<DT, ET>>,
    cancel_token: CancellationToken,
    stats: Arc<Mutex<Stats>>,
    data: WorkerState<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let concurrency = queue_config.concurrency;
    let (result_tx, result_rx) = mpsc::channel::<Result<(), ET>>(concurrency);
    let (job_tx, mut job_rx) = mpsc::channel::<WorkerJob>(concurrency);
    let semaphores = Arc::new(SemaphoresMap::new(concurrency));

    tokio::spawn(result_collector::run(
        result_rx,
        cancel_token.clone(),
        config.clone(),
        stats.clone(),
    ));
    tokio::spawn(run_queue_watcher(
        config.clone(),
        cancel_token.clone(),
        queue_config.clone(),
        job_tx.clone(),
        semaphores.clone(),
    ));

    loop {
        select! {
            job = job_rx.recv() => {
                if let Some(job) = job {
                    tokio::spawn(process_job(
                        config.clone(),
                        data.clone(),
                        result_tx.clone(),
                        job,
                    ));
                }
            }
            _ = cancel_token.cancelled() => {
                break;
            }
        }
    }

    wait_for_workers_to_finish(semaphores.clone()).await;

    Ok(())
}

async fn process_job<DT, ET>(
    config: Arc<Config<DT, ET>>,
    data: WorkerState<DT>,
    result_tx: mpsc::Sender<Result<(), ET>>,
    job_event: WorkerJob,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let envelope: JobEnvelope = match config.storage.get(&job_event.job_id).await {
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

    let data = data.clone();
    let result_tx = result_tx.clone();
    let result = executor::run(config, job, envelope, data)
        .await
        .expect("Failed to run job");
    drop(job_event.permit);
    result_tx.send(result).await.ok();
}

async fn run_queue_watcher<DT, ET>(
    config: Arc<Config<DT, ET>>,
    cancel_token: CancellationToken,
    queue_config: QueueConfig,
    job_tx: mpsc::Sender<WorkerJob>,
    semaphores: Arc<SemaphoresMap>,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let mut tracked_queues = HashSet::new();

    let mut redis_manager = config.build_redis_manager().await;

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
                config.clone(),
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

async fn wait_for_workers_to_finish(semaphores: Arc<SemaphoresMap>) {
    let mut ticks = 0;

    loop {
        ticks += 1;

        let busy_count = semaphores.busy_count().await;
        if busy_count == 0 {
            break;
        }

        if ticks % 200 == 0 {
            tracing::info!("Waiting for {} workers to finish...", busy_count);
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
