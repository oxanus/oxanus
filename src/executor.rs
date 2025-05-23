use crate::job_envelope::JobEnvelope;
use crate::worker_state::WorkerState;
use crate::{storage, worker::BoxedWorker};

pub async fn run<DT: Send + Sync + Clone + 'static, ET: std::error::Error + Send + Sync>(
    redis: redis::aio::ConnectionManager,
    job_name: String,
    job: BoxedWorker<DT, ET>,
    envelope: JobEnvelope,
    data: WorkerState<DT>,
) -> Result<(), ET> {
    tracing::info!(
        job_id = envelope.id,
        queue = envelope.job.queue,
        job = job_name,
        "Job started"
    );
    let start = std::time::Instant::now();
    let result = job.process(&data).await;
    let duration = start.elapsed();
    let is_err = result.is_err();
    tracing::info!(
        job_id = envelope.id,
        queue = envelope.job.queue,
        job = job_name,
        success = !is_err,
        duration = duration.as_millis(),
        retries = envelope.meta.retries,
        "Job finished"
    );
    let max_retries = job.max_retries();
    let retry_delay = job.retry_delay(envelope.meta.retries);

    if is_err {
        if envelope.meta.retries < max_retries {
            storage::retry_in(&redis, envelope, retry_delay)
                .await
                .expect("Failed to retry job");
        } else {
            tracing::error!("Job {} failed after {} retries", envelope.id, max_retries);
            storage::kill(&redis, &envelope)
                .await
                .expect("Failed to kill job");
        }
    } else {
        storage::finish(&redis, &envelope)
            .await
            .expect("Failed to finish job");
    }

    result
}
