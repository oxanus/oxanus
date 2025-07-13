use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use crate::context::{ContextValue, JobState};
use crate::job_envelope::JobEnvelope;
use crate::worker::BoxedWorker;
use crate::{Config, Context, OxanusError};

#[derive(Debug)]
enum ExecutionResult<ET> {
    NotPanic(Result<(), ET>),
    Panic(String),
}

pub(crate) enum ExecutionError<ET> {
    NotPanic(ET),
    Panic(),
}

pub async fn run<DT, ET>(
    config: Arc<Config<DT, ET>>,
    worker: BoxedWorker<DT, ET>,
    envelope: &JobEnvelope,
    ctx: ContextValue<DT>,
) -> Result<Result<(), ExecutionError<ET>>, OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    tracing::info!(
        job_id = envelope.id,
        queue = envelope.queue,
        worker = envelope.job.name,
        "Job started"
    );
    let start = std::time::Instant::now();
    let full_ctx = Context {
        ctx: ctx.0,
        meta: envelope.meta.clone(),
        state: JobState::new(
            config.storage.clone(),
            envelope.id.clone(),
            envelope.meta.state.clone(),
        ),
    };

    // Process the job and handle panics
    let result = match AssertUnwindSafe(process(&worker, full_ctx, envelope))
        .catch_unwind()
        .await
    {
        Ok(result) => ExecutionResult::NotPanic(result),
        Err(panic) => {
            let panic_msg = if let Some(s) = panic.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = panic.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic occurred".to_string()
            };
            ExecutionResult::Panic(panic_msg)
        }
    };

    let duration = start.elapsed();
    let is_err = !matches!(result, ExecutionResult::NotPanic(Ok(_)));
    tracing::info!(
        job_id = envelope.id,
        queue = envelope.queue,
        job = envelope.job.name,
        success = !is_err,
        duration = duration.as_millis(),
        retries = envelope.meta.retries,
        "Job finished"
    );

    let max_retries = worker.max_retries();
    let retry_delay = worker.retry_delay(envelope.meta.retries);

    match result {
        ExecutionResult::NotPanic(result) => {
            match &result {
                Ok(()) => {
                    if let Err(e) = config.storage.internal.finish_with_success(envelope).await {
                        tracing::error!("Failed to finish job: {}", e);
                    }
                }
                Err(e) => {
                    #[cfg(feature = "sentry")]
                    sentry_core::capture_error(e);

                    tracing::error!(
                        job_id = envelope.id,
                        queue = envelope.queue,
                        worker = envelope.job.name,
                        "Job failed"
                    );

                    handle_err(config, &e.to_string(), envelope, retry_delay, max_retries).await;
                }
            }

            Ok(result.map_err(ExecutionError::NotPanic))
        }
        ExecutionResult::Panic(panic_msg) => {
            #[cfg(feature = "sentry")]
            sentry_core::capture_message(&panic_msg, sentry_core::Level::Error);

            handle_err(config, &panic_msg, envelope, retry_delay, max_retries).await;

            Ok(Err(ExecutionError::Panic()))
        }
    }
}

#[tracing::instrument(skip_all, name = "job", fields(
    job_id = envelope.id,
    queue = envelope.queue,
    worker = envelope.job.name,
    args = %envelope.job.args,
    success,
))]
async fn process<DT, ET>(
    worker: &BoxedWorker<DT, ET>,
    full_ctx: Context<DT>,
    envelope: &JobEnvelope,
) -> Result<(), ET>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let span = tracing::Span::current();
    let result = worker.process(&full_ctx).await;

    span.record("success", result.is_ok());

    result
}

async fn handle_err<DT, ET>(
    config: Arc<Config<DT, ET>>,
    err_msg: &str,
    envelope: &JobEnvelope,
    retry_delay: u64,
    max_retries: u32,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    if envelope.meta.retries < max_retries {
        if let Err(e) = config.storage.internal.finish_with_failure(envelope).await {
            tracing::error!("Failed to finish job: {}", e);
        }
        if let Err(e) = config
            .storage
            .internal
            .retry_in(envelope.id.clone(), retry_delay)
            .await
        {
            tracing::error!("Failed to retry job: {}", e);
        }
    } else {
        tracing::error!(
            "Job {} failed after {} retries: {}",
            envelope.id,
            max_retries,
            err_msg
        );
        if let Err(e) = config.storage.internal.kill(envelope).await {
            tracing::error!("Failed to kill job: {}", e);
        }
    }
}
