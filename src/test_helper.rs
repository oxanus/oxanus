use rand::distr::{Alphanumeric, SampleString};

pub fn random_string() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 16)
}

pub async fn redis_pool() -> Result<deadpool_redis::Pool, deadpool_redis::PoolError> {
    dotenvy::from_filename(".env.test").ok();
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let cfg = deadpool_redis::Config::from_url(redis_url);
    let pool = cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .unwrap();

    Ok(pool)
}
