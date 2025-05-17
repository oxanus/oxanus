use crate::queue::{QueueDynamic, QueueStatic};

#[derive(Clone)]
pub struct Processor {
    pub concurrency: usize,
    pub queues: Vec<ProcessorQueue>,
}

#[derive(Clone)]
pub enum ProcessorQueue {
    Queue(String),
    Pattern(String),
}

pub enum ProcessorConcurrency {
    PerQueue(usize),
    Global(usize),
}

impl Processor {
    pub fn new() -> Self {
        Self {
            concurrency: 1,
            queues: Vec::new(),
        }
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn queue(mut self, queue: &str) -> Self {
        self.queues.push(ProcessorQueue::Queue(queue.to_string()));
        self
    }

    pub fn queue_static(mut self, queue: &QueueStatic) -> Self {
        self.queues.push(ProcessorQueue::Queue(queue.name.clone()));
        self
    }

    pub fn queue_dynamic(mut self, queue: &QueueDynamic) -> Self {
        self.queues
            .push(ProcessorQueue::Pattern(format!("{}*", queue.prefix)));
        self
    }
}
