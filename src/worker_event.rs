use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub enum WorkerEvent {
    Job {
        queue: String,
        job: serde_json::Value,
        permit: OwnedSemaphorePermit,
    },
    Exit,
}
