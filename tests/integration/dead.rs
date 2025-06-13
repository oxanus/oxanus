use oxanus::Queue;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerFail {}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerFail {
    type Context = ();
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerContext { .. }: &oxanus::WorkerContext<()>,
    ) -> Result<(), WorkerError> {
        Err(WorkerError::GenericError(
            "I have nothing to live for...".to_string(),
        ))
    }

    fn retry_delay(&self, _retries: u32) -> u64 {
        0
    }

    fn max_retries(&self) -> u32 {
        0
    }
}

#[tokio::test]
pub async fn test_dead() -> TestResult {
    let ctx = oxanus::WorkerContextValue::new(());
    let storage = oxanus::Storage::from_env().namespace(random_string());
    let config = oxanus::Config::new(storage.clone())
        .register_queue::<QueueOne>()
        .register_worker::<WorkerFail>()
        .exit_when_processed(1);

    oxanus::enqueue(&storage, QueueOne, WorkerFail {}).await?;

    assert_eq!(storage.enqueued_count(&QueueOne.key()).await?, 1);

    oxanus::run(config, ctx).await?;

    assert_eq!(storage.dead_count().await?, 1);
    assert_eq!(storage.enqueued_count(&QueueOne.key()).await?, 0);

    Ok(())
}
