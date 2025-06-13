use crate::shared::*;
use oxanus::Queue;
use redis::AsyncCommands;
use testresult::TestResult;

#[tokio::test]
pub async fn test_standard() -> TestResult {
    let redis_client = setup();

    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client.clone()).await?;
    let ctx = oxanus::WorkerContextValue::new(WorkerState {
        redis: redis_manager.clone(),
    });

    let storage = oxanus::Storage::new(redis_client).namespace(random_string());
    let config = oxanus::Config::new(storage.clone())
        .register_queue::<QueueOne>()
        .register_worker::<WorkerRedisSet>()
        .exit_when_processed(1);

    let random_key = uuid::Uuid::new_v4().to_string();
    let random_value = uuid::Uuid::new_v4().to_string();

    oxanus::enqueue(
        &storage,
        QueueOne,
        WorkerRedisSet {
            key: random_key.clone(),
            value: random_value.clone(),
        },
    )
    .await?;

    assert_eq!(storage.enqueued_count(&QueueOne.key()).await?, 1);

    oxanus::run(config, ctx).await?;

    let value: Option<String> = redis_manager.get(random_key).await?;

    assert_eq!(value, Some(random_value));
    assert_eq!(storage.enqueued_count(&QueueOne.key()).await?, 0);

    Ok(())
}
