use redis::AsyncCommands;

use crate::OxanusError;

// pub async fn rpush<T>(
//     redis: &redis::aio::ConnectionManager,
//     key: &str,
//     value: &T,
// ) -> Result<(), OxanusError>
// where
//     T: serde::Serialize,
// {
//     let mut redis = redis.clone();
//     let value_str = serde_json::to_string(value)?;
//     let _: i32 = redis.rpush(key, value_str).await?;
//     Ok(())
// }

pub async fn rpush_many<T>(
    redis: &redis::aio::ConnectionManager,
    key: &str,
    values: &[T],
) -> Result<(), OxanusError>
where
    T: serde::Serialize,
{
    let mut redis = redis.clone();
    let mut values_str = Vec::new();
    for value in values {
        values_str.push(serde_json::to_string(value)?);
    }
    let _: i32 = redis.rpush(key, values_str).await?;
    Ok(())
}

pub async fn zadd_with_delay<T>(
    redis: &redis::aio::ConnectionManager,
    key: &str,
    delay: u64,
    value: &T,
) -> Result<(), OxanusError>
where
    T: serde::Serialize,
{
    let mut redis = redis.clone();
    let now = chrono::Utc::now().timestamp_micros();
    let value_str = serde_json::to_string(value)?;
    let _: i32 = redis
        .zadd(key, value_str, now + 1_000_000 * delay as i64)
        .await?;
    Ok(())
}

pub async fn zadd_with_timestamp<T>(
    redis: &redis::aio::ConnectionManager,
    key: &str,
    timestamp: i64,
    value: &T,
) -> Result<(), OxanusError>
where
    T: serde::Serialize,
{
    let mut redis = redis.clone();
    let value_str = serde_json::to_string(value)?;
    let _: i32 = redis.zadd(key, value_str, timestamp).await?;
    Ok(())
}
