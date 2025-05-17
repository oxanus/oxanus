#[derive(Debug, Clone)]
pub struct QueueStatic {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct QueueDynamic {
    pub prefix: String,
}

impl QueueStatic {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

impl QueueDynamic {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }

    pub fn to_static(&self, suffix: &str) -> QueueStatic {
        QueueStatic {
            name: format!("{}{}", self.prefix, suffix),
        }
    }
}
