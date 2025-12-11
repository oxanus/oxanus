use crate::{OxanusError, Storage, storage_internal::StorageInternal};

#[must_use]
pub struct StorageBuilder {
    namespace: Option<String>,
    max_pool_size: Option<usize>,
    timeouts: Option<StorageBuilderTimeouts>,
}

pub struct StorageBuilderTimeouts {
    pub wait: Option<std::time::Duration>,
    pub create: Option<std::time::Duration>,
    pub recycle: Option<std::time::Duration>,
}

impl StorageBuilderTimeouts {
    pub fn new(timeout: std::time::Duration) -> Self {
        Self {
            wait: Some(timeout),
            create: Some(timeout),
            recycle: Some(timeout),
        }
    }
}

impl Default for StorageBuilderTimeouts {
    fn default() -> Self {
        Self {
            wait: Some(std::time::Duration::from_millis(300)),
            create: Some(std::time::Duration::from_millis(300)),
            recycle: Some(std::time::Duration::from_millis(300)),
        }
    }
}

impl Into<deadpool_redis::Timeouts> for StorageBuilderTimeouts {
    fn into(self) -> deadpool_redis::Timeouts {
        deadpool_redis::Timeouts {
            wait: self.wait,
            create: self.create,
            recycle: self.recycle,
        }
    }
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
            max_pool_size: None,
            timeouts: None,
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn max_pool_size(mut self, max_pool_size: usize) -> Self {
        self.max_pool_size = Some(max_pool_size);
        self
    }

    pub fn timeouts(mut self, timeouts: StorageBuilderTimeouts) -> Self {
        self.timeouts = Some(timeouts);
        self
    }

    pub fn build_from_redis_url(self, url: impl Into<String>) -> Result<Storage, OxanusError> {
        let mut cfg = deadpool_redis::Config::from_url(url);
        cfg.pool = Some(deadpool_redis::PoolConfig {
            max_size: self.max_pool_size.unwrap_or(50),
            timeouts: self.timeouts.unwrap_or_default().into(),
            queue_mode: Default::default(),
        });

        let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        Ok(Storage {
            internal: StorageInternal::new(pool, self.namespace),
        })
    }

    pub fn build_from_env(self) -> Result<Storage, OxanusError> {
        self.build_from_env_var("REDIS_URL")
    }

    pub fn build_from_env_var(self, var_name: &str) -> Result<Storage, OxanusError> {
        let url = std::env::var(var_name).unwrap_or_else(|_| panic!("{var_name} is not set"));
        self.build_from_redis_url(url)
    }

    pub fn build_from_pool(self, pool: deadpool_redis::Pool) -> Result<Storage, OxanusError> {
        let internal = StorageInternal::new(pool, self.namespace);
        Ok(Storage { internal })
    }
}
