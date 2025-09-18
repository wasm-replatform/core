use rdkafka::consumer::{BaseConsumer, StreamConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::client::ClientContext;
use rdkafka::message::{Message as KafkaMessage, Headers};
use rdkafka::topic_partition_list::TopicPartitionList;
use futures::StreamExt;
use tokio::sync::watch;
use crate::{KafkaConfig, ProviderError, SchemaConfig, SRClient, Message};

pub struct KafkaConsumer {
    inner: StreamConsumer<ConsumerCallbackLogger>,
    shutdown_tx: watch::Sender<bool>,
    schema_registry: Option<SRClient>,
}

impl KafkaConsumer {
    pub fn new(cfg: &KafkaConfig, schema_cfg: Option<SchemaConfig>) -> Result<Self, ProviderError> {
        let mut client = ClientConfig::new();
        let context = ConsumerCallbackLogger;
        client.set("bootstrap.servers", &cfg.brokers);

        if let (Some(user), Some(pass)) = (cfg.username.as_ref(), cfg.password.as_ref()) {
            client.set("security.protocol", "SASL_PLAINTEXT");
            client.set("sasl.mechanisms", "PLAIN");
            client.set("sasl.username", user);
            client.set("sasl.password", pass);
        }

        if let Some(g) = cfg.group_id.as_ref() {
            client.set("group.id", g);
        }

        let consumer: StreamConsumer<ConsumerCallbackLogger> = client
            .create_with_context(context)
            .expect("Consumer creation failed");

        let (tx, _rx) = watch::channel(false);

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
            inner: consumer, 
            shutdown_tx: tx,
            schema_registry: sr_client
        })
    }

    pub async fn start<F, Fut>(&self, topics: &[&str], handler: F) -> Result<(), ProviderError>
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), ProviderError>> + Send,
    {
        self.inner
            .subscribe(topics)
            .map_err(|e| ProviderError::ConsumerError(format!("{:?}", e)))?;
    
        let mut stream = self.inner.stream();
    
        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Ok(kmsg) => {
                    // Convert OwnedMessage to your Message type
                    let owned_msg = kmsg.detach();
                    let payload_bytes = owned_msg.payload()
                        .map(|p| p.to_vec())    
                        .unwrap_or_else(Vec::new);
                    let payload = if let Some(sr) = &self.schema_registry {
                        // schema_registry exists → serialize
                        sr.validate_and_encode_json(&owned_msg.topic(), payload_bytes).await?
                    } else {
                        // no schema_registry → use raw payload
                        owned_msg.payload().map(|p| p.to_vec()).unwrap_or_else(Vec::new)
                    };
                    let message = Message {
                        topic: owned_msg.topic().to_string(),
                        partition: Some(owned_msg.partition()),
                        offset: Some(owned_msg.offset()),
                        key: owned_msg.key().map(|k| k.to_vec()),
                        payload: payload,
                        headers: owned_msg.headers().map(|hdrs| {
                            hdrs.iter()
                                .filter_map(|h| {
                                    h.value.map(|v| (h.key.to_string(), v.to_vec()))
                                })
                                .collect::<Vec<_>>()
                            }),
                    };

    
                    // Call handler with OwnedMessage
                    match handler(message).await {
                        Ok(()) => {
                            let _ = self.inner.commit_message(&kmsg, rdkafka::consumer::CommitMode::Async);
                        }
                        Err(err) => {
                            tracing::error!("Handler error: {:?}", err);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Kafka consumer error: {:?}", e);
                }
            }
        }
    
        Ok(())
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

struct ConsumerCallbackLogger;

impl ClientContext for ConsumerCallbackLogger {}

impl ConsumerContext for ConsumerCallbackLogger {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}