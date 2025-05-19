use crate::{JobEnvelope, OxanusError, redis_helper, timed_migrator};

const RETRIES_QUEUE: &str = "oxanus:retries";

pub async fn run(redis_client: redis::Client) {
    tracing::info!("Starting retrying migrator");

    tokio::spawn(timed_migrator::run(
        redis_client.clone(),
        RETRIES_QUEUE.to_string(),
    ));
}

pub async fn retry_in(
    redis: &redis::aio::ConnectionManager,
    envelope: JobEnvelope,
    retry_in: u64,
) -> Result<(), OxanusError> {
    redis_helper::zadd_with_delay(redis, RETRIES_QUEUE, retry_in, &envelope).await
}
