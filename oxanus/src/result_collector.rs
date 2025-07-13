use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

use crate::{OxanusError, config::Config, job_envelope::JobEnvelope};

#[derive(Default, Debug)]
pub struct Stats {
    pub processed: u64,
    pub succeeded: u64,
    pub panicked: u64,
    pub failed: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobResult {
    pub kind: JobResultKind,
    pub envelope: JobEnvelope,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobResultKind {
    Success,
    Panicked,
    Failed,
}

pub async fn run<DT, ET>(
    mut rx: mpsc::Receiver<JobResult>,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Some(result) => update_stats(Arc::clone(&config), Arc::clone(&stats), result).await?,
                    None => return Ok(()),
                }
            }
            _ = config.cancel_token.cancelled() => {
                return Ok(());
            }
        }
    }
}

async fn update_stats<DT, ET>(
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    result: JobResult,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let processed = {
        let mut stats = stats.lock().await;
        stats.processed += 1;
        match result.kind {
            JobResultKind::Success => stats.succeeded += 1,
            JobResultKind::Panicked => {
                stats.panicked += 1;
                stats.failed += 1;
            }
            JobResultKind::Failed => stats.failed += 1,
        }

        stats.processed
    };

    config.storage.internal.update_stats(result).await?;

    if let Some(exit_when_processed) = config.exit_when_processed {
        if processed >= exit_when_processed {
            config.cancel_token.cancel();
        }
    }

    Ok(())
}
