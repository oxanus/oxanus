use std::str::FromStr;
use std::{any::type_name, collections::HashMap};

use crate::error::OxanusError;
use crate::worker::Worker;

type BoxedJob<DT, ET> = Box<dyn Worker<Context = DT, Error = ET>>;
type JobFactory<DT, ET> = fn(serde_json::Value) -> Result<BoxedJob<DT, ET>, OxanusError>;

pub struct WorkerRegistry<DT, ET> {
    jobs: HashMap<String, JobFactory<DT, ET>>,
    pub schedules: HashMap<String, CronJob>,
}

#[derive(Debug, Clone)]
pub struct CronJob {
    pub schedule: cron::Schedule,
    pub queue_key: String,
}

impl<DT, ET> WorkerRegistry<DT, ET> {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            schedules: HashMap::new(),
        }
    }

    pub fn register<T>(&mut self) -> &mut Self
    where
        T: Worker<Context = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
    {
        fn factory<
            T: Worker<Context = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
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

    pub fn register_cron<T>(&mut self, schedule: &str, queue_key: String) -> &mut Self
    where
        T: Worker<Context = DT, Error = ET> + serde::de::DeserializeOwned + 'static,
    {
        let name = type_name::<T>();
        let schedule = cron::Schedule::from_str(schedule)
            .expect(&format!("{}: Invalid cron schedule: {}", name, schedule));

        self.register::<T>();
        self.schedules.insert(
            name.to_string(),
            CronJob {
                schedule,
                queue_key,
            },
        );
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

impl<DT, ET> Default for WorkerRegistry<DT, ET> {
    fn default() -> Self {
        Self::new()
    }
}
