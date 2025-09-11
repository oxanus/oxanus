use crate::config::Config;
use crate::context::{ContextValue, JobState};
use crate::error::OxanusError;
use crate::{Context, JobId, Queue};

enum ProcessJobResult {
    Success,
    Failed,
    Missing,
}

#[derive(Default, Debug)]
pub struct DrainStats {
    pub processed: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub missing: u64,
}

/// Drains a queue of jobs.
///
/// This function will drain a queue of jobs, processing them one by one.
///
/// It is useful in development or testing to process a queue of jobs without running the full worker.
///
/// # Arguments
///
/// * `config` - The worker configuration, including queue and worker registrations
/// * `ctx` - The context value that will be shared across all worker instances
/// * `queue` - The queue to drain
///
/// # Returns
///
/// Returns statistics about the drain operation, or an [`OxanusError`] if the operation fails.
pub async fn drain<DT, ET>(
    config: &Config<DT, ET>,
    ctx: ContextValue<DT>,
    queue: impl Queue,
) -> Result<DrainStats, OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let queue_key = queue.key();
    let mut stats = DrainStats::default();

    while let Some(job_id) = config.storage.internal.dequeue(&queue_key).await? {
        let result = process_job(config, ctx.clone(), job_id).await?;
        match result {
            ProcessJobResult::Success => stats.succeeded += 1,
            ProcessJobResult::Failed => stats.failed += 1,
            ProcessJobResult::Missing => stats.missing += 1,
        }
        stats.processed += 1;
    }

    Ok(stats)
}

async fn process_job<DT, ET>(
    config: &Config<DT, ET>,
    ctx: ContextValue<DT>,
    job_id: JobId,
) -> Result<ProcessJobResult, OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let envelope = match config.storage.internal.get_job(&job_id).await? {
        Some(envelope) => envelope,
        None => return Ok(ProcessJobResult::Missing),
    };

    let job = config
        .registry
        .build(&envelope.job.name, envelope.job.args.clone())?;

    let full_ctx = Context {
        ctx: ctx.0,
        meta: envelope.meta.clone(),
        state: JobState::new(config.storage.clone(), job_id, envelope.meta.state.clone()),
    };

    let job_result = job.process(&full_ctx).await;

    match job_result {
        Ok(()) => {
            config
                .storage
                .internal
                .finish_with_success(&envelope)
                .await?;
            Ok(ProcessJobResult::Success)
        }
        Err(e) => {
            tracing::error!("Job failed: {}", e);
            config
                .storage
                .internal
                .finish_with_failure(&envelope)
                .await?;
            config.storage.internal.kill(&envelope).await?;
            Ok(ProcessJobResult::Failed)
        }
    }
}
