use crate::{JobEnvelope, OxanusError, redis_helper, timed_migrator};

const SCHEDULED_QUEUE: &str = "oxanus:scheduled";

pub async fn run(redis_client: redis::Client) {
    tracing::info!("Starting scheduler");

    tokio::spawn(timed_migrator::run(
        redis_client.clone(),
        SCHEDULED_QUEUE.to_string(),
    ));
}

pub async fn enqueue_in(
    redis: &redis::aio::ConnectionManager,
    envelope: JobEnvelope,
    delay: u64,
) -> Result<(), OxanusError> {
    redis_helper::zadd_with_delay(redis, SCHEDULED_QUEUE, delay, &envelope).await
}
