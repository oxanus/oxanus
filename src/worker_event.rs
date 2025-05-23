use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub enum WorkerEvent {
    Job(WorkerEventJob),
}

#[derive(Debug)]
pub struct WorkerEventJob {
    pub job_id: String,
    pub permit: OwnedSemaphorePermit,
}
