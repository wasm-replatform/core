use api::Handler;

use crate::{KafkaConfig, Message};

pub struct HandlerDriver {
    config: KafkaConfig,
}

impl HandlerDriver {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    pub fn run<I, O, P>(&self, provider: P)
    where
        I: Handler<Vec<O>, P>,
        I: From<Message>,
        O: Into<Message>,
    {
    }
}
