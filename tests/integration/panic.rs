use crate::shared::*;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

#[derive(Serialize, Deserialize)]
struct WorkerPanic {}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerPanic {
    type Context = ();
    type Error = std::io::Error;

    async fn process(&self, _: &oxanus::Context<()>) -> Result<(), std::io::Error> {
        panic!("test panic");
    }

    fn max_retries(&self) -> u32 {
        0
    }
}

#[tokio::test]
pub async fn test_panic() -> TestResult {
    let redis_pool = setup();
    let ctx = oxanus::Context::value(());
    let storage = oxanus::Storage::builder()
        .from_redis_pool(redis_pool)
        .namespace(random_string())
        .build()?;
    let config = oxanus::Config::new(&storage)
        .register_queue::<QueueOne>()
        .register_worker::<WorkerPanic>()
        .exit_when_processed(1);

    storage.enqueue(QueueOne, WorkerPanic {}).await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    let stats = oxanus::run(config, ctx).await?;

    assert_eq!(stats.panicked, 1);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 0);
    assert_eq!(storage.dead_count().await?, 1);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}
