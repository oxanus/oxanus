use crate::shared::*;
use redis::AsyncCommands;
use testresult::TestResult;

#[tokio::test]
pub async fn main() -> TestResult {
    setup();

    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let redis_client = redis::Client::open(redis_url.clone()).expect("Failed to open Redis client");
    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client.clone()).await?;
    let data = oxanus::WorkerState::new(WorkerState {
        redis: redis_manager.clone(),
    });

    let storage = oxanus::Storage::new(redis_client).namespace(random_string());
    let config = oxanus::Config::new(storage)
        .register_queue::<QueueOne>()
        .register_worker::<WorkerRedisSet>()
        .exit_when_processed(1);

    let random_key = uuid::Uuid::new_v4().to_string();
    let random_value = uuid::Uuid::new_v4().to_string();

    oxanus::enqueue(
        &config,
        QueueOne,
        WorkerRedisSet {
            key: random_key.clone(),
            value: random_value.clone(),
        },
    )
    .await?;

    oxanus::run(config, data).await?;

    let value: Option<String> = redis_manager.get(random_key).await?;

    assert_eq!(value, Some(random_value));

    Ok(())
}
