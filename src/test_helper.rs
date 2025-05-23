use rand::distr::{Alphanumeric, SampleString};

pub fn random_string() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 16)
}

pub async fn redis_manager() -> Result<redis::aio::ConnectionManager, redis::RedisError> {
    dotenvy::from_filename(".env.test").ok();
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let redis_client = redis::Client::open(redis_url.clone()).expect("Failed to open Redis client");
    let redis_manager = redis::aio::ConnectionManager::new(redis_client).await?;
    Ok(redis_manager)
}
