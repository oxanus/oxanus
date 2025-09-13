use crate::{OxanusError, Storage, storage_internal::StorageInternal};

pub struct StorageBuilder {
    namespace: Option<String>,
    config: Option<deadpool_redis::Config>,
    max_pool_size: Option<usize>,
    pool: Option<deadpool_redis::Pool>,
    firehose: Option<bool>,
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
            config: None,
            max_pool_size: None,
            pool: None,
            firehose: None,
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn firehose(mut self, firehose: bool) -> Self {
        self.firehose = Some(firehose);
        self
    }

    pub fn max_pool_size(mut self, max_pool_size: usize) -> Self {
        self.max_pool_size = Some(max_pool_size);
        if let Some(cfg) = &mut self.config
            && let Some(pool_cfg) = &mut cfg.pool
        {
            pool_cfg.max_size = max_pool_size;
        }
        self
    }

    pub fn from_redis_url(mut self, url: impl Into<String>) -> Result<Self, OxanusError> {
        let mut cfg = deadpool_redis::Config::from_url(url);
        cfg.pool = Some(deadpool_redis::PoolConfig {
            max_size: self.max_pool_size.unwrap_or(100),
            timeouts: deadpool_redis::Timeouts {
                wait: Some(std::time::Duration::from_millis(300)),
                create: Some(std::time::Duration::from_millis(300)),
                recycle: Some(std::time::Duration::from_millis(300)),
            },
            queue_mode: Default::default(),
        });

        self.config = Some(cfg);
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
        let url = std::env::var(var_name).unwrap_or_else(|_| panic!("{var_name} is not set"));
        self.from_redis_url(url)
    }

    pub fn build(self) -> Result<Storage, OxanusError> {
        let internal = match (self.pool, self.config) {
            (Some(pool), None) => StorageInternal::new(pool, self.namespace),
            (None, Some(config)) => {
                let pool = config.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
                StorageInternal::new(pool, self.namespace)
            }
            (None, None) => {
                return Err(OxanusError::ConfigError(
                    "You must provide a Redis pool or a Redis config".to_string(),
                ));
            }
            (Some(_), Some(_)) => {
                return Err(OxanusError::ConfigError(
                    "Redis pool and config cannot be set at the same time".to_string(),
                ));
            }
        };

        Ok(Storage { internal })
    }
}
