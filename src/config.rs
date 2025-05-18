use crate::queue::{QueueConfig, QueueConfigTrait};
use crate::worker::Worker;
use crate::worker_registry::WorkerRegistry;

pub struct Config<DT, ET> {
    pub registry: WorkerRegistry<DT, ET>,
    pub queues: Vec<QueueConfig>,
    pub exit_when_finished: Option<u64>,
}

impl<DT, ET> Config<DT, ET> {
    pub fn new() -> Self {
        Self {
            registry: WorkerRegistry::new(),
            queues: Vec::new(),
            exit_when_finished: None,
        }
    }

    pub fn register_queue<Q>(mut self) -> Self
    where
        Q: QueueConfigTrait,
    {
        self.queues.push(Q::to_config());
        self
    }

    pub fn register_queue_with_concurrency<Q>(mut self, concurrency: usize) -> Self
    where
        Q: QueueConfigTrait,
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

    pub fn exit_when_finished(mut self, exit_when_finished: u64) -> Self {
        self.exit_when_finished = Some(exit_when_finished);
        self
    }
}
