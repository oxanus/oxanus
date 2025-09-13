use crate::OxanusError;
use deadpool_redis::redis;

pub struct Throttler {
    redis_pool: deadpool_redis::Pool,
    key: String,
    limit: u64,
    window_ms: i64,
}

#[derive(Debug)]
pub struct ThrottlerState {
    pub requests: u64,
    pub is_allowed: bool,
    pub throttled_for: Option<i64>,
}

impl Throttler {
    pub fn new(redis_pool: deadpool_redis::Pool, key: &str, limit: u64, window_ms: i64) -> Self {
        Throttler {
            redis_pool,
            key: Self::build_key(key),
            limit,
            window_ms,
        }
    }

    pub async fn consume(&self) -> Result<ThrottlerState, OxanusError> {
        let mut redis = self.redis_pool.get().await?;
        let current_time = u64::try_from(chrono::Utc::now().timestamp_micros())?;
        let state = self.state_w_conn(&mut redis).await?;

        if state.is_allowed {
            let (updated, _): (u64, ()) = redis::pipe()
                .zadd(&self.key, current_time, current_time)
                .expire(&self.key, self.window_s())
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

    pub async fn state(&self) -> Result<ThrottlerState, OxanusError> {
        let mut redis = self.redis_pool.get().await?;
        self.state_w_conn(&mut redis).await
    }

    async fn state_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
    ) -> Result<ThrottlerState, OxanusError> {
        let now = chrono::Utc::now().timestamp_micros();
        let window_start = now - self.window_micros();

        let (_, first, request_count): ((), Vec<(String, f64)>, u64) = redis::pipe()
            .zrembyscore(&self.key, 0, window_start)
            .zrange_withscores(&self.key, 0, 0)
            .zcard(&self.key)
            .query_async(&mut *redis)
            .await?;

        let accurate_window_start = if let Some((_, score)) = first.first() {
            Some(*score as i64)
        } else {
            None
        };

        let is_allowed = request_count < self.limit;

        let throttled_for_micros = if is_allowed {
            None
        } else {
            accurate_window_start.map(|start| now - start + self.window_micros())
        };

        let throttled_for = throttled_for_micros.map(|micros| micros / 1000 + 1);

        Ok(ThrottlerState {
            requests: request_count,
            is_allowed,
            throttled_for,
        })
    }

    fn build_key(key: &str) -> String {
        format!("oxanus:throttler:{key}")
    }

    fn window_s(&self) -> i64 {
        self.window_ms / 1000
    }

    fn window_micros(&self) -> i64 {
        self.window_ms * 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helper::*;
    use testresult::TestResult;

    #[tokio::test]
    async fn test_consume() -> TestResult {
        let pool = redis_pool().await?;
        let key = random_string();
        let rate_limiter = Throttler::new(pool, &key, 2, 60000);
        assert!(rate_limiter.consume().await?.is_allowed);
        assert!(rate_limiter.consume().await?.is_allowed);
        let state = rate_limiter.consume().await?;
        assert!(!state.is_allowed);
        assert!(state.throttled_for.is_some());
        assert!(state.throttled_for.unwrap() >= 60000);
        let state = rate_limiter.consume().await?;
        assert!(!state.is_allowed);
        assert!(state.throttled_for.is_some());
        assert!(state.throttled_for.unwrap() >= 60000);

        Ok(())
    }
}
