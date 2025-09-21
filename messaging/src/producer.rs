use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::Message as KafkaMessage;
use rdkafka::producer::{ThreadedProducer, BaseRecord, ProducerContext};
use rdkafka::client::ClientContext;
use crate::{KafkaConfig, SchemaConfig, DeliveryInfo, MessagingError, Message, SRClient, Partitioner};



/// KafkaProducer using ThreadedProducer with retry
pub struct KafkaProducer {
    inner: ThreadedProducer<ProduceCallbackLogger>,
    schema_registry: Option<SRClient>,
    partitioner: Option<Partitioner>,
}

impl KafkaProducer {
    pub fn new(cfg: &KafkaConfig, schema_cfg: Option<SchemaConfig>) -> Result<Self, MessagingError> {

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

        // Optional: Initialize custom partitioner if js_partitioner is true
        let partitioner = if cfg.js_partitioner.unwrap_or(false) {
            if let Some(part_count) = cfg.partition_count {
                Some(Partitioner::new(part_count))
            } else {
                None
            }
        } else {
            None
        };    
            
        // Initialize schema registry client if config is provided
        let sr_client = if let Some(cfg) = &schema_cfg {
            if cfg.url.is_empty() {
                None
            } else {
                Some(SRClient::new(cfg.clone()))
            }
        } else {
            None
        };

        Ok(Self { 
            inner: producer, 
            schema_registry: sr_client,
            partitioner,
        })
    }

    /// Send message once
    pub async fn send_once(
        &self,
        msg: Message,
    ) -> Result<DeliveryInfo, MessagingError> {
        let payload = if let Some(sr) = &self.schema_registry {
            // schema_registry exists → serialize
            sr.validate_and_encode_json(&msg.topic, msg.payload).await?
        } else {
            // no schema_registry → use raw payload
            msg.payload.clone()
        };

        let mut record = BaseRecord::to(msg.topic.as_str())
            .payload(&payload)
            .key(msg.key.as_deref().unwrap_or(&[]));

        // Set partition if provided, else use custom partitioner if available    
        if let Some(p) = msg.partition {
            record = record.partition(p);
        }else if let Some(partitioner) = &self.partitioner {
            if let Some(key) = msg.key.as_deref() {
                let partition = partitioner.partition(key);
                record = record.partition(partition);
            }
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
    pub async fn send_with_retry(
        &self,
        msg: Message,
        max_retries: usize,
        retry_delay: Duration,
    ) -> Result<DeliveryInfo, MessagingError> {
        for attempt in 0..=max_retries {
            match self.send_once(msg.clone()).await {
                Ok(info) => return Ok(info),
                Err(err) => {
                    if attempt == max_retries {
                        return Err(err);
                    }
                    tracing::error!(
                        "Send failed (attempt {}/{}): {:?}, retrying...",
                        attempt + 1,
                        max_retries,
                        err
                    );
                    tokio::time::sleep(retry_delay).await; // use async sleep
                }
            }
        };
        Err(MessagingError::SendFailed(
            "Maximum retries exceeded".to_string(),
        ))
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
                tracing::debug!(
                    "produced message with key {} in offset {} of partition {}",
                    key,
                    msg.offset(),
                    msg.partition()
                )
            }
            Err((producer_err, message)) => {
                // Wrap KafkaError in MessagingError
                let provider_err = MessagingError::Kafka(producer_err.clone());
                let key: &str = message.key_view().unwrap().unwrap();

                 // Log or forward the structured error
                 tracing::error!(
                    "Failed to produce message with key '{}': {}",
                    key,
                    provider_err
                );
            }
        }
    }
}
