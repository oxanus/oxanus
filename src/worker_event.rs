use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub struct WorkerJob {
    pub job_id: String,
    pub permit: OwnedSemaphorePermit,
}
