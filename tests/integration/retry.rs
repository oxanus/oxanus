use crate::shared::*;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerRedisSetWithRetry {
    pub key: String,
    pub value_first: String,
    pub value_second: String,
}

#[async_trait::async_trait]
impl oxanus::Worker for WorkerRedisSetWithRetry {
    type Data = WorkerState;
    type Error = WorkerError;

    async fn process(
        &self,
        oxanus::WorkerState(conns): &oxanus::WorkerState<WorkerState>,
    ) -> Result<(), WorkerError> {
        let mut redis = conns.redis.clone();
        let value: Option<String> = redis.get(&self.key).await?;
        if value.is_some() {
            let _: () = redis
                .set_ex(&self.key, self.value_second.clone(), 3)
                .await?;
            return Ok(());
        }
        let _: () = redis.set_ex(&self.key, self.value_first.clone(), 3).await?;
        Err(WorkerError::GenericError("Key not set".to_string()))
    }

    fn retry_delay(&self, _retries: u32) -> u64 {
        0
    }

    fn max_retries(&self) -> u32 {
        1
    }
}

#[tokio::test]
pub async fn main() -> TestResult {
    setup();

    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let redis_client = redis::Client::open(redis_url.clone()).expect("Failed to open Redis client");
    let mut redis_manager = redis::aio::ConnectionManager::new(redis_client.clone()).await?;
    let data = oxanus::WorkerState::new(WorkerState {
        redis: redis_manager.clone(),
    });

    let config = oxanus::Config::new(redis_client.clone())
        .register_queue::<QueueOne>()
        .register_worker::<WorkerRedisSetWithRetry>()
        .exit_when_processed(2);

    let random_key = uuid::Uuid::new_v4().to_string();
    let random_value_first = uuid::Uuid::new_v4().to_string();
    let random_value_second = uuid::Uuid::new_v4().to_string();

    oxanus::enqueue(
        &config,
        QueueOne,
        WorkerRedisSetWithRetry {
            key: random_key.clone(),
            value_first: random_value_first.clone(),
            value_second: random_value_second.clone(),
        },
    )
    .await?;

    oxanus::run(config, data).await?;

    let value: Option<String> = redis_manager.get(random_key).await?;

    assert_eq!(value, Some(random_value_second));

    Ok(())
}
