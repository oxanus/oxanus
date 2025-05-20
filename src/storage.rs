use redis::AsyncCommands;

use crate::{JobEnvelope, OxanusError};

const JOBS_KEY: &str = "oxanus:jobs";
const DEAD_QUEUE: &str = "oxanus:dead";
pub const SCHEDULE_QUEUE: &str = "oxanus:schedule";
pub const RETRY_QUEUE: &str = "oxanus:retry";

pub async fn enqueue(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();
    let (_, _): ((), i32) = redis::pipe()
        .hset(JOBS_KEY, &envelope.id, serde_json::to_string(envelope)?)
        .rpush(&envelope.job.queue, &envelope.id)
        .query_async(&mut redis)
        .await?;
    Ok(())
}

pub async fn enqueue_in(
    redis: &redis::aio::ConnectionManager,
    envelope: JobEnvelope,
    delay_s: u64,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();
    let (_, _): ((), ()) = redis::pipe()
        .hset(JOBS_KEY, &envelope.id, serde_json::to_string(&envelope)?)
        .zadd(
            SCHEDULE_QUEUE,
            envelope.id,
            envelope.job.created_at + delay_s * 1_000_000,
        )
        .query_async(&mut redis)
        .await?;
    Ok(())
}

pub async fn retry_in(
    redis: &redis::aio::ConnectionManager,
    envelope: JobEnvelope,
    delay_s: u64,
) -> Result<(), OxanusError> {
    let updated_envelope = envelope.with_retries_incremented();
    let mut redis = redis.clone();
    let (_, _): ((), ()) = redis::pipe()
        .hset(
            JOBS_KEY,
            &updated_envelope.id,
            serde_json::to_string(&updated_envelope)?,
        )
        .zadd(
            RETRY_QUEUE,
            updated_envelope.id,
            updated_envelope.job.created_at + delay_s * 1_000_000,
        )
        .query_async(&mut redis)
        .await?;
    Ok(())
}

pub async fn get(
    redis: &redis::aio::ConnectionManager,
    id: &str,
) -> Result<JobEnvelope, OxanusError> {
    let mut redis = redis.clone();
    let envelope: String = redis.hget(JOBS_KEY, id).await?;
    Ok(serde_json::from_str(&envelope)?)
}

pub async fn get_many(
    redis: &redis::aio::ConnectionManager,
    ids: &[String],
) -> Result<Vec<JobEnvelope>, OxanusError> {
    let mut redis = redis.clone();
    let mut cmd = redis::cmd("HMGET");
    cmd.arg(JOBS_KEY);
    cmd.arg(ids);
    let envelopes_str: Vec<String> = cmd.query_async(&mut redis).await?;
    let mut envelopes: Vec<JobEnvelope> = vec![];
    for envelope_str in envelopes_str {
        envelopes.push(serde_json::from_str(&envelope_str)?);
    }
    Ok(envelopes)
}

pub async fn kill(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();
    let _: () = redis::pipe()
        .hdel(JOBS_KEY, &envelope.id)
        .rpush(DEAD_QUEUE, &serde_json::to_string(envelope)?)
        .query_async(&mut redis)
        .await?;
    Ok(())
}

pub async fn delete(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();
    let _: () = redis.hdel(JOBS_KEY, &envelope.id).await?;
    Ok(())
}
