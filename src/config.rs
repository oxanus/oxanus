use signal_hook::consts::{SIGINT, SIGTERM};
use tokio_util::sync::CancellationToken;

use crate::queue::{Queue, QueueConfig};
use crate::storage::Storage;
use crate::worker::Worker;
use crate::worker_registry::WorkerRegistry;

#[derive(Clone)]
pub struct Config<DT, ET> {
    pub registry: WorkerRegistry<DT, ET>,
    pub queues: Vec<QueueConfig>,
    pub exit_when_finished: bool,
    pub exit_when_processed: Option<u64>,
    pub shutdown_signals: Vec<i32>,
    pub cancel_token: CancellationToken,
    pub storage: Storage,
}

impl<DT, ET> Config<DT, ET> {
    pub fn new(redis_client: redis::Client) -> Self {
        Self {
            registry: WorkerRegistry::new(),
            queues: Vec::new(),
            exit_when_finished: false,
            exit_when_processed: None,
            shutdown_signals: vec![SIGINT, SIGTERM],
            cancel_token: CancellationToken::new(),
            storage: Storage::new(redis_client),
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
        T: Worker<Data = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
    {
        self.registry.register::<T>();
        self
    }

    pub fn exit_when_processed(mut self, processed: u64) -> Self {
        self.exit_when_processed = Some(processed);
        self
    }

    pub fn with_graceful_shutdown(mut self, signals: impl IntoIterator<Item = i32>) -> Self {
        self.shutdown_signals = signals.into_iter().collect();
        self
    }
}
