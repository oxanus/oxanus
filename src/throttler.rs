use crate::OxanusError;

pub struct Throttler {
    redis: redis::aio::ConnectionManager,
    key: String,
    limit: u64,
    window_ms: u64,
}

#[derive(Debug)]
pub struct ThrottlerState {
    pub requests: u64,
    pub is_allowed: bool,
    pub throttled_for: Option<u64>,
}

impl Throttler {
    pub fn new(
        redis: redis::aio::ConnectionManager,
        key: &str,
        limit: u64,
        window_ms: u64,
    ) -> Self {
        Throttler {
            redis,
            key: Self::build_key(&key),
            limit,
            window_ms,
        }
    }

    pub async fn consume(&self) -> Result<ThrottlerState, OxanusError> {
        let mut redis = self.redis.clone();
        let current_time = u64::try_from(chrono::Utc::now().timestamp_millis())?;
        let state = self.state(&mut redis).await?;

        if state.is_allowed {
            let (updated, _): (u64, ()) = redis::pipe()
                .zadd(&self.key, current_time, current_time)
                .expire(&self.key, i64::try_from(self.window_s())?)
                .query_async(&mut redis)
                .await?;

            Ok(ThrottlerState {
                requests: state.requests + updated,
                ..state
            })
        } else {
            Ok(state)
        }
    }

    // pub async fn is_allowed(&self) -> Result<bool, OxanusError> {
    //     let mut redis = self.redis.clone();
    //     let state = self.state(&mut redis).await?;

    //     Ok(state.is_allowed)
    // }

    pub async fn state(
        &self,
        redis: &mut redis::aio::ConnectionManager,
    ) -> Result<ThrottlerState, OxanusError> {
        let now = u64::try_from(chrono::Utc::now().timestamp_millis())?;
        let window_start = now - self.window_ms;

        let (_, first, request_count): ((), Vec<(String, f64)>, u64) = redis::pipe()
            .zrembyscore(&self.key, 0, window_start)
            .zrange_withscores(&self.key, 0, 0)
            .zcard(&self.key)
            .query_async(redis)
            .await?;

        let accurate_window_start = if let Some((_, score)) = first.first() {
            Some(u64::try_from(*score as i64)?)
        } else {
            None
        };

        let is_allowed = request_count < self.limit;

        let throttled_for = if is_allowed {
            None
        } else {
            accurate_window_start.map(|start| now - start + self.window_ms + 1)
        };

        Ok(ThrottlerState {
            requests: request_count,
            is_allowed,
            throttled_for,
        })
    }

    fn build_key(key: &str) -> String {
        format!("throttler:{}", key)
    }

    fn window_s(&self) -> u64 {
        self.window_ms / 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distr::{Alphanumeric, SampleString};
    use testresult::TestResult;

    pub fn random_string() -> String {
        Alphanumeric.sample_string(&mut rand::rng(), 16)
    }

    async fn redis() -> Result<redis::aio::ConnectionManager, redis::RedisError> {
        let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
        let redis_client =
            redis::Client::open(redis_url.clone()).expect("Failed to open Redis client");
        let redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;
        Ok(redis_manager)
    }

    #[tokio::test]
    async fn test_consume() -> TestResult {
        let redis = redis().await?;
        let key = random_string();
        let rate_limiter = Throttler::new(redis, &key, 2, 60000);
        assert!(rate_limiter.consume().await?.is_allowed);
        assert!(rate_limiter.consume().await?.is_allowed);
        assert!(!rate_limiter.consume().await?.is_allowed);
        assert!(!rate_limiter.consume().await?.is_allowed);

        Ok(())
    }
}
