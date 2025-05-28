use std::sync::Arc;

use crate::job_envelope::JobEnvelope;
use crate::worker::BoxedWorker;
use crate::worker_state::WorkerState;
use crate::{Config, OxanusError};

pub async fn run<DT, ET>(
    config: Arc<Config<DT, ET>>,
    worker: BoxedWorker<DT, ET>,
    envelope: JobEnvelope,
    data: WorkerState<DT>,
) -> Result<Result<(), ET>, OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    tracing::info!(
        job_id = envelope.id,
        queue = envelope.job.queue,
        job = envelope.job.name,
        "Job started"
    );
    let start = std::time::Instant::now();
    let result = worker.process(&data).await;
    let duration = start.elapsed();
    let is_err = result.is_err();
    tracing::info!(
        job_id = envelope.id,
        queue = envelope.job.queue,
        job = envelope.job.name,
        success = !is_err,
        duration = duration.as_millis(),
        retries = envelope.meta.retries,
        "Job finished"
    );
    let max_retries = worker.max_retries();
    let retry_delay = worker.retry_delay(envelope.meta.retries);

    if is_err {
        if envelope.meta.retries < max_retries {
            config
                .storage
                .finish_with_failure(&envelope)
                .await
                .expect("Failed to finish job");
            config
                .storage
                .retry_in(envelope, retry_delay)
                .await
                .expect("Failed to retry job");
        } else {
            tracing::error!("Job {} failed after {} retries", envelope.id, max_retries);
            config
                .storage
                .kill(&envelope)
                .await
                .expect("Failed to kill job");
        }
    } else {
        config
            .storage
            .finish_with_success(&envelope)
            .await
            .expect("Failed to finish job");
    }

    Ok(result)
}
