use api::Handler;

use crate::consumer::KafkaConsumer;
use crate::producer::KafkaProducer;
use crate::{KafkaConfig, Message, MessagingError};

pub struct HandlerDriver {
    config: KafkaConfig,
}

impl HandlerDriver {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    pub async fn run<I, O, P>(
        &self, provider: P, from_message: impl Fn(Message) -> I + Clone + Send + Sync,
        to_message: impl Fn(O) -> Vec<Message> + Send + Sync,
    ) -> Result<(), MessagingError>
    where
        I: Handler<O, P>,
        P: Send + Sync + 'static,
        <I as Handler<O, P>>::Error: std::fmt::Debug,
    {
        let consumer = KafkaConsumer::new(&self.config, None)?;
        let producer = KafkaProducer::new(&self.config, None)?;

        consumer.start(&["foo"], |message| async move {
            let input = from_message(message);
            let output = input.handle("", &provider).await.unwrap();
            let msgs = to_message(output.body);
            for msg in msgs {
                producer.send_once(msg).await?;
            }
            Ok(())
        });

        Ok(())
    }
}
