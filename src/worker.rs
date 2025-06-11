use crate::worker_context::WorkerContext;

pub type BoxedWorker<DT, ET> = Box<dyn Worker<Context = DT, Error = ET>>;

#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    type Context: Clone + Send + Sync;
    type Error: std::error::Error + Send + Sync;

    async fn process(&self, data: &WorkerContext<Self::Context>) -> Result<(), Self::Error>;

    fn max_retries(&self) -> u32 {
        1
    }

    fn retry_delay(&self, retries: u32) -> u64 {
        // 0 -> 25 seconds
        // 1 -> 125 seconds
        // 2 -> 625 seconds
        // 3 -> 3125 seconds
        // 4 -> 15625 seconds
        // 5 -> 78125 seconds
        // 6 -> 390625 seconds
        // 7 -> 1953125 seconds
        u64::pow(5, retries + 2)
    }

    fn unique_id(&self) -> Option<String> {
        None
    }
}
