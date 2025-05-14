pub trait Queue: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn concurrency(&self) -> usize;

    fn config(&self) -> QueueConfig {
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
