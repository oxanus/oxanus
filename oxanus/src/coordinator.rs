use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;

use crate::config::Config;
use crate::context::ContextValue;
use crate::error::OxanusError;
use crate::executor::ExecutionError;
use crate::job_envelope::JobEnvelope;
use crate::queue::{QueueConfig, QueueKind};
use crate::result_collector::{JobResult, JobResultKind};
use crate::semaphores_map::SemaphoresMap;
use crate::worker_event::WorkerJob;
use crate::{
    dispatcher, executor,
    result_collector::{self, Stats},
};

pub async fn run<DT, ET>(
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    ctx: ContextValue<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let concurrency = queue_config.concurrency;
    let (result_tx, result_rx) = mpsc::channel::<JobResult>(concurrency);
    let (job_tx, mut job_rx) = mpsc::channel::<WorkerJob>(concurrency);
    let semaphores = Arc::new(SemaphoresMap::new(concurrency));
    let mut joinset = JoinSet::new();

    joinset.spawn(result_collector::run(
        result_rx,
        Arc::clone(&config),
        Arc::clone(&stats),
    ));
    joinset.spawn(run_queue_watcher(
        Arc::clone(&config),
        queue_config.clone(),
        job_tx.clone(),
        Arc::clone(&semaphores),
    ));

    loop {
        tokio::select! {
            job = job_rx.recv() => {
                if let Some(job) = job {
                    joinset.spawn(process_job(
                        Arc::clone(&config),
                        ctx.clone(),
                        result_tx.clone(),
                        job,
                    ));
                }
            }
            Some(task_result) = joinset.join_next() => {
                task_result??;
            }
            _ = config.cancel_token.cancelled() => {
                break;
            }
        }
    }

    wait_for_workers_to_finish(config, Arc::clone(&semaphores)).await;

    Ok(())
}

async fn process_job<DT, ET>(
    config: Arc<Config<DT, ET>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<JobResult>,
    job_event: WorkerJob,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    tracing::trace!("Processing job: {:?}", job_event);

    let envelope: JobEnvelope = match config.storage.internal.get_job(&job_event.job_id).await {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            tracing::warn!("Job {} not found", job_event.job_id);
            if let Err(e) = config.storage.internal.delete_job(&job_event.job_id).await {
                #[cfg(feature = "sentry")]
                sentry_core::capture_error(&e);
                tracing::error!("Failed to delete job: {}", e);
            }
            return Ok(());
        }
        Err(e) => {
            #[cfg(feature = "sentry")]
            sentry_core::capture_error(&e);
            tracing::error!("Failed to get job envelope: {}", e);
            return Ok(());
        }
    };

    tracing::debug!(
        job_id = envelope.id,
        latency_ms = envelope.meta.latency_millis(),
        envelope = %serde_json::to_value(&envelope)?,
        "Received envelope"
    );
    let job = match config
        .registry
        .build(&envelope.job.name, envelope.job.args.clone())
    {
        Ok(job) => job,
        Err(e) => {
            tracing::error!("Invalid job: {} - {}", &envelope.job.name, e);
            if let Err(e) = config.storage.internal.kill(&envelope).await {
                #[cfg(feature = "sentry")]
                sentry_core::capture_error(&e);
                tracing::error!("Failed to kill job: {}", e);
            }
            return Ok(());
        }
    };

    let result = executor::run(config, job, &envelope, ctx.clone()).await?;
    drop(job_event.permit);

    process_result(result_tx, result, envelope).await;

    Ok(())
}

async fn process_result<ET>(
    result_tx: mpsc::Sender<JobResult>,
    result: Result<(), ExecutionError<ET>>,
    envelope: JobEnvelope,
) where
    ET: std::error::Error + Send + Sync + 'static,
{
    let kind = match result {
        Ok(()) => JobResultKind::Success,
        Err(e) => match e {
            ExecutionError::NotPanic(_) => JobResultKind::Failed,
            ExecutionError::Panic() => JobResultKind::Panicked,
        },
    };

    result_tx.send(JobResult { envelope, kind }).await.ok();
}


async fn run_queue_watcher<DT, ET>(
    config: Arc<Config<DT, ET>>,
    queue_config: QueueConfig,
    job_tx: mpsc::Sender<WorkerJob>,
    semaphores: Arc<SemaphoresMap>,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let mut tracked_queues = HashSet::new();

    loop {
        let all_queues: HashSet<String> = match &queue_config.kind {
            QueueKind::Static { key } => HashSet::from([key.clone()]),
            QueueKind::Dynamic { prefix, .. } => {
                config
                    .storage
                    .internal
                    .queue_keys(&format!("{prefix}*"))
                    .await?
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
                Arc::clone(&config),
                queue_config.clone(),
                queue.clone(),
                job_tx.clone(),
                Arc::clone(&semaphores),
            ));

            tracked_queues.insert(queue);
        }

        if config.cancel_token.is_cancelled() {
            return Ok(());
        } else if let QueueKind::Dynamic { sleep_period, .. } = queue_config.kind {
            tokio::time::sleep(sleep_period).await;
        } else {
            return Ok(());
        }
    }
}

async fn wait_for_workers_to_finish<DT, ET>(
    config: Arc<Config<DT, ET>>,
    semaphores: Arc<SemaphoresMap>,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let t_start = std::time::Instant::now();
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

        if t_start.elapsed() > config.shutdown_timeout {
            tracing::error!("Shutdown timeout reached");
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
