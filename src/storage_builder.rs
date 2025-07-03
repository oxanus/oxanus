use crate::{OxanusError, Storage, storage_internal::StorageInternal};

pub struct StorageBuilder {
    namespace: Option<String>,
    pool: Option<deadpool_redis::Pool>,
    pool_size: Option<usize>,
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBuilder {
    pub fn new() -> Self {
        Self {
            namespace: None,
            pool: None,
            pool_size: None,
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = Some(pool_size);
        self
    }

    pub fn from_redis_url(mut self, url: impl Into<String>) -> Result<Self, OxanusError> {
        let mut cfg = deadpool_redis::Config::from_url(url);
        cfg.pool = Some(deadpool_redis::PoolConfig {
            max_size: self.pool_size.unwrap_or(50),
            timeouts: deadpool_redis::Timeouts {
                wait: Some(std::time::Duration::from_millis(100)),
                create: Some(std::time::Duration::from_millis(100)),
                recycle: Some(std::time::Duration::from_millis(100)),
            },
            ..Default::default()
        });
        let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;

        self.pool = Some(pool);
        Ok(self)
    }

    pub fn from_redis_pool(mut self, pool: deadpool_redis::Pool) -> Self {
        self.pool = Some(pool);
        self
    }

    pub fn from_env(self) -> Result<Self, OxanusError> {
        self.from_env_var("REDIS_URL")
    }

    pub fn from_env_var(self, var_name: &str) -> Result<Self, OxanusError> {
        let url = std::env::var(var_name).unwrap_or_else(|_| panic!("{} is not set", var_name));
        self.from_redis_url(url)
    }

    pub fn build(self) -> Result<Storage, OxanusError> {
        let pool = self.pool.ok_or(OxanusError::ConfigRedisNotConfigured)?;
        let internal = StorageInternal::new(pool, self.namespace);

        Ok(Storage { internal })
    }
}
