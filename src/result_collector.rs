use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

use crate::config::Config;

#[derive(Default, Debug)]
pub struct Stats {
    pub processed: u64,
    pub succeeded: u64,
    pub panicked: u64,
    pub failed: u64,
}

pub(crate) enum JobResultType {
    Success,
    Panicked,
    Failed,
}

pub async fn run<DT, ET>(
    mut rx: mpsc::Receiver<JobResultType>,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Some(result) => update_stats(config.clone(), stats.clone(), result).await,
                    None => break,
                }
            }
            _ = config.cancel_token.cancelled() => {
                break;
            }
        }
    }
}

async fn update_stats<DT, ET>(
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    result: JobResultType,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let processed = {
        let mut stats = stats.lock().await;
        stats.processed += 1;
        match result {
            JobResultType::Success => stats.succeeded += 1,
            JobResultType::Panicked => {
                stats.panicked += 1;
                stats.failed += 1;
            }
            JobResultType::Failed => stats.failed += 1,
        }

        stats.processed
    };

    if let Some(exit_when_processed) = config.exit_when_processed {
        if processed >= exit_when_processed {
            config.cancel_token.cancel();
        }
    }
}
