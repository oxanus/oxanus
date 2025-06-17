use std::pin::Pin;
use tokio_util::sync::CancellationToken;

use crate::Storage;
use crate::queue::{Queue, QueueConfig};
use crate::worker::Worker;
use crate::worker_registry::WorkerRegistry;

pub struct Config<DT, ET> {
    pub(crate) registry: WorkerRegistry<DT, ET>,
    pub(crate) queues: Vec<QueueConfig>,
    pub(crate) exit_when_processed: Option<u64>,
    pub(crate) shutdown_signal:
        Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send + Sync>>,
    pub(crate) cancel_token: CancellationToken,
    pub storage: Storage,
}

impl<DT, ET> Config<DT, ET> {
    pub fn new(storage: &Storage) -> Self {
        Self {
            registry: WorkerRegistry::new(),
            queues: Vec::new(),
            exit_when_processed: None,
            shutdown_signal: Box::pin(default_shutdown_signal()),
            cancel_token: CancellationToken::new(),
            storage: storage.clone(),
        }
    }

    pub fn register_queue<Q>(mut self) -> Self
    where
        Q: Queue,
    {
        self.queues.push(Q::to_config());
        self
    }

    pub fn register_queue_with_concurrency<Q>(mut self, concurrency: usize) -> Self
    where
        Q: Queue,
    {
        let mut config = Q::to_config();
        config.concurrency = concurrency;
        self.queues.push(config);
        self
    }

    pub fn register_worker<T>(mut self) -> Self
    where
        T: Worker<Context = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
    {
        self.registry.register::<T>();
        self
    }

    pub fn register_cron_worker<T>(mut self, schedule: &str, queue: impl Queue) -> Self
    where
        T: Worker<Context = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
    {
        self.registry.register_cron::<T>(schedule, queue.key());
        self
    }

    pub fn exit_when_processed(mut self, processed: u64) -> Self {
        self.exit_when_processed = Some(processed);
        self
    }

    pub fn with_graceful_shutdown(
        mut self,
        fut: impl Future<Output = Result<(), std::io::Error>> + Send + Sync + 'static,
    ) -> Self {
        self.shutdown_signal = Box::pin(fut);
        self
    }

    pub fn consume_shutdown_signal(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send + Sync + 'static>> {
        let mut shutdown_signal = no_signal();
        std::mem::swap(&mut self.shutdown_signal, &mut shutdown_signal);
        shutdown_signal
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn default_shutdown_signal() -> Result<(), std::io::Error> {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    tokio::select! {
        _ = ctrl_c => Ok(()),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(target_os = "windows")]
async fn default_shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

fn no_signal() -> Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send + Sync + 'static>>
{
    Box::pin(async move { Ok(()) })
}
