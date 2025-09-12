use std::time::Duration;
use std::thread;

use rdkafka::config::ClientConfig;
use rdkafka::Message as KafkaMessage;
use rdkafka::producer::{ThreadedProducer, BaseRecord, ProducerContext};
use rdkafka::client::ClientContext;
use crate::{KafkaConfig, DeliveryInfo, ProviderError, Message};

/// KafkaProducer using ThreadedProducer with retry
pub struct KafkaProducer {
    inner: ThreadedProducer<ProduceCallbackLogger>,
}

impl KafkaProducer {
    pub fn new(cfg: &KafkaConfig) -> Result<Self, ProviderError> {

        let binding = ClientConfig::new();
        let mut config = binding;
        config.set("bootstrap.servers", &cfg.brokers);

        if let (Some(user), Some(pass)) = (cfg.username.as_ref(), cfg.password.as_ref()) {
            config.set("security.protocol", "SASL_PLAINTEXT");
            config.set("sasl.mechanisms", "PLAIN");
            config.set("sasl.username", user);
            config.set("sasl.password", pass);
        }

        let producer = config
            .create_with_context(ProduceCallbackLogger {})
            .expect("invalid producer config");

        Ok(Self {
            inner: producer,
        })
    }

    /// Send message once
    pub fn send_once(
        &self,
        msg: Message,
    ) -> Result<DeliveryInfo, ProviderError> {

        let mut record = BaseRecord::to(msg.topic.as_str())
            .payload(&msg.payload)
            .key(msg.key.as_deref().unwrap_or(&[]));

        if let Some(p) = msg.partition {
            record = record.partition(p);
        }

        let send_result = self.inner.send(record);

        match send_result {
            Ok(_) => Ok(DeliveryInfo {
                topic: msg.topic,
                partition: msg.partition.unwrap_or(-1),
                offset: msg.offset.unwrap_or(0),
                error: None,
            }),
            Err((err, _)) => Ok(DeliveryInfo {
                topic: msg.topic,
                partition: msg.partition.unwrap_or(-1),
                offset: msg.offset.unwrap_or(0),
                error: Some(format!("{:?}", err)),
            }),
        }
    }

    /// Send with retry
    pub fn send_with_retry(
        &self,
        msg: Message,
        max_retries: usize,
        retry_delay: Duration,
    ) -> Result<DeliveryInfo, ProviderError> {
        for attempt in 0..=max_retries {
            match self.send_once(msg.clone()) {
                Ok(info) => return Ok(info),
                Err(err) => {
                    if attempt == max_retries {
                        return Err(err);
                    }
                    eprintln!("Send failed (attempt {}/{}): {:?}, retrying...", attempt + 1, max_retries, err);
                    thread::sleep(retry_delay);
                }
            }
        }
        unreachable!()
    }
}

struct ProduceCallbackLogger;

impl ClientContext for ProduceCallbackLogger {}

impl ProducerContext for ProduceCallbackLogger {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::producer::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        let dr = delivery_result.as_ref();
        //let msg = dr.unwrap();

        match dr {
            Ok(msg) => {
                let key: &str = msg.key_view().unwrap().unwrap();
                println!(
                    "produced message with key {} in offset {} of partition {}",
                    key,
                    msg.offset(),
                    msg.partition()
                )
            }
            Err(producer_err) => {
                let key: &str = producer_err.1.key_view().unwrap().unwrap();

                println!(
                    "failed to produce message with key {} - {}",
                    key, producer_err.0,
                )
            }
        }
    }
}
