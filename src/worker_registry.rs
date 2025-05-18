use std::{any::type_name, collections::HashMap};

use crate::error::OxanusError;
use crate::worker::Worker;

type BoxedJob<DT, ET> = Box<dyn Worker<Data = DT, Error = ET>>;
type JobFactory<DT, ET> = fn(serde_json::Value) -> Result<BoxedJob<DT, ET>, OxanusError>;

#[derive(Clone)]
pub struct WorkerRegistry<DT, ET> {
    jobs: HashMap<String, JobFactory<DT, ET>>,
}

impl<DT, ET> WorkerRegistry<DT, ET> {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn register<T>(&mut self) -> &mut Self
    where
        T: Worker<Data = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
    {
        fn factory<
            T: Worker<Data = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
            DT,
            ET,
        >(
            value: serde_json::Value,
        ) -> Result<BoxedJob<DT, ET>, OxanusError> {
            let job: T = serde_json::from_value(value)?;
            Ok(Box::new(job))
        }

        let name = type_name::<T>();

        self.jobs.insert(name.to_string(), factory::<T, DT, ET>);
        self
    }

    pub fn build(
        &self,
        name: &str,
        json: serde_json::Value,
    ) -> Result<BoxedJob<DT, ET>, OxanusError> {
        let factory = self.jobs.get(name).ok_or_else(|| {
            OxanusError::GenericError(format!("Job type {} not registered", name))
        })?;
        match factory(json) {
            Ok(job) => Ok(job),
            Err(e) => Err(OxanusError::JobFactoryError(format!(
                "Failed to build job {}: {}",
                name, e
            ))),
        }
    }
}
