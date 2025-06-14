use crate::{OxanusError, Storage, StorageInternal};

pub struct StorageBuilder {
    namespace: Option<String>,
    pool: Option<deadpool_redis::Pool>,
}

impl StorageBuilder {
    pub fn new() -> Self {
        Self {
            namespace: None,
            pool: None,
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn from_redis_url(mut self, url: impl Into<String>) -> Result<Self, OxanusError> {
        let cfg = deadpool_redis::Config::from_url(url);
        let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;

        self.pool = Some(pool);
        Ok(self)
    }

    pub fn from_redis_pool(mut self, pool: deadpool_redis::Pool) -> Self {
        self.pool = Some(pool);
        self
    }

    pub fn from_env(self) -> Result<Self, OxanusError> {
        let url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
        self.from_redis_url(url)
    }

    pub fn build(self) -> Result<Storage, OxanusError> {
        let pool = self.pool.ok_or(OxanusError::ConfigRedisNotConfigured)?;
        let internal = StorageInternal::new(pool, self.namespace);

        Ok(Storage { internal })
    }
}
