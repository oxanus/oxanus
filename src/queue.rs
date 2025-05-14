#[derive(Debug, Clone, Copy)]
pub struct Queue {
    name: &'static str,
    concurrency: usize,
}

impl Queue {
    pub const fn new(name: &'static str, concurrency: usize) -> Self {
        Self { name, concurrency }
    }

    pub const fn name(&self) -> &str {
        &self.name
    }

    pub const fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn config(&self) -> QueueConfig {
        QueueConfig {
            name: self.name().to_string(),
            concurrency: self.concurrency(),
        }
    }
}

pub struct QueueConfig {
    pub name: String,
    pub concurrency: usize,
}
