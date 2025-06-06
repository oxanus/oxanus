use std::sync::Arc;
use tokio::select;
use tokio::sync::{Mutex, mpsc};

use crate::config::Config;

#[derive(Default, Debug)]
pub struct Stats {
    pub processed: u64,
    pub succeeded: u64,
    pub failed: u64,
}

pub async fn run<DT, ET>(
    mut rx: mpsc::Receiver<Result<(), ET>>,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    loop {
        select! {
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
    result: Result<(), ET>,
) where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let processed = {
        let mut stats = stats.lock().await;
        stats.processed += 1;
        match result {
            Ok(_) => stats.succeeded += 1,
            Err(_e) => stats.failed += 1,
        }

        stats.processed
    };

    if let Some(exit_when_processed) = config.exit_when_processed {
        if processed >= exit_when_processed {
            config.cancel_token.cancel();
        }
    }
}
