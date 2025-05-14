use crate::queue::Queue;
use crate::worker_state::WorkerState;

#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    type Data: Clone;
    type Error: std::error::Error + Send + Sync;

    async fn process(&self, data: &WorkerState<Self::Data>) -> Result<(), Self::Error>;

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

    fn queue(&self) -> Box<dyn Queue>;
}
