use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub enum WorkerEvent {
    Job {
        queue: String,
        job_id: String,
        permit: OwnedSemaphorePermit,
    },
    Exit,
}
