use crate::{JobEnvelope, OxanusError, redis_helper};

const DEAD_QUEUE: &str = "oxanus:dead";

pub async fn add(
    redis: &redis::aio::ConnectionManager,
    envelope: JobEnvelope,
) -> Result<(), OxanusError> {
    redis_helper::zadd_with_timestamp(redis, DEAD_QUEUE, envelope.meta.created_at, &envelope).await
}
