use redis::AsyncCommands;

use crate::{JobEnvelope, OxanusError};

pub const SCHEDULE_QUEUE: &str = "oxanus:schedule";
pub const RETRY_QUEUE: &str = "oxanus:retry";
const JOBS_KEY: &str = "oxanus:jobs";
const DEAD_QUEUE: &str = "oxanus:dead";
const JOB_EXPIRE_TIME: i64 = 7 * 24 * 3600; // 7 days

pub async fn enqueue(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();

    if should_skip_job(&mut redis, envelope).await? {
        tracing::warn!("Unique job {} already exists, skipping", envelope.id);
        return Ok(());
    }

    let (_, _): ((), i32) = redis::pipe()
        .hset(JOBS_KEY, &envelope.id, serde_json::to_string(envelope)?)
        .hexpire(
            JOBS_KEY,
            JOB_EXPIRE_TIME,
            redis::ExpireOption::NONE,
            &envelope.id,
        )
        .rpush(&envelope.job.queue, &envelope.id)
        .query_async(&mut redis)
        .await?;

    Ok(())
}

async fn should_skip_job(
    redis: &mut redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
) -> Result<bool, OxanusError> {
    if !envelope.meta.unique {
        return Ok(false);
    }

    let exists: bool = redis.hexists(JOBS_KEY, &envelope.id).await?;
    Ok(exists)
}

pub async fn enqueue_in(
    redis: &redis::aio::ConnectionManager,
    envelope: &JobEnvelope,
    delay_s: u64,
) -> Result<(), OxanusError> {
    let mut redis = redis.clone();

    if should_skip_job(&mut redis, &envelope).await? {
        tracing::warn!("Unique job {} already exists, skipping", envelope.id);
        return Ok(());
    }

    let (_, _): ((), ()) = redis::pipe()
        .hset(JOBS_KEY, &envelope.id, serde_json::to_string(&envelope)?)
        .hexpire(
            JOBS_KEY,
            JOB_EXPIRE_TIME,
            redis::ExpireOption::NONE,
            &envelope.id,
        )
        .zadd(
            SCHEDULE_QUEUE,
            &envelope.id,
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
        .hexpire(
            JOBS_KEY,
            JOB_EXPIRE_TIME,
            redis::ExpireOption::NONE,
            &updated_envelope.id,
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
) -> Result<Option<JobEnvelope>, OxanusError> {
    let mut redis = redis.clone();
    let envelope: Option<String> = redis.hget(JOBS_KEY, id).await?;
    match envelope {
        Some(envelope) => Ok(Some(serde_json::from_str(&envelope)?)),
        None => Ok(None),
    }
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
