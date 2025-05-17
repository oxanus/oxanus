use std::collections::HashMap;

// use crate::queue::Queue;
use crate::Processor;
use crate::worker::Worker;
use crate::worker_registry::WorkerRegistry;

pub struct Config<DT, ET> {
    pub registry: WorkerRegistry<DT, ET>,
    // pub queues: HashMap<String, Queue>,
    pub processors: Vec<Processor>,
    pub exit_when_idle: bool,
    pub exit_when_finished: Option<u64>,
}

impl<DT, ET> Config<DT, ET> {
    pub fn new() -> Self {
        Self {
            registry: WorkerRegistry::new(),
            // queues: HashMap::new(),
            processors: Vec::new(),
            exit_when_idle: false,
            exit_when_finished: None,
        }
    }

    pub fn register_processor(mut self, processor: Processor) -> Self {
        self.processors.push(processor);
        self
    }

    // pub fn register_queue(mut self, queue: Queue) -> Self {
    //     self.queues.insert(queue.name().to_string(), queue);
    //     self
    // }

    pub fn register_worker<T>(mut self) -> Self
    where
        T: Worker<Data = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
    {
        self.registry.register::<T>();
        self
    }

    pub fn exit_when_idle(mut self) -> Self {
        self.exit_when_idle = true;
        self
    }

    pub fn exit_when_finished(mut self, exit_when_finished: u64) -> Self {
        self.exit_when_finished = Some(exit_when_finished);
        self
    }
}
