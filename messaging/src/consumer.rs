use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedMessage;
use futures::StreamExt;
use tokio::sync::watch;
use crate::{KafkaConfig, ProviderError};

pub struct KafkaConsumer {
    inner: StreamConsumer,
    shutdown_tx: watch::Sender<bool>,
}

impl KafkaConsumer {
    pub fn new(cfg: &KafkaConfig) -> Result<Self, ProviderError> {
        let mut client = ClientConfig::new();
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

        let consumer: StreamConsumer = client.create().map_err(ProviderError::Kafka)?;
        let (tx, _rx) = watch::channel(false);

        Ok(Self { inner: consumer, shutdown_tx: tx })
    }

    pub async fn start<F, Fut>(&self, topics: &[&str], handler: F) -> Result<(), ProviderError>
    where
        F: Fn(OwnedMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), ProviderError>> + Send,
    {
        self.inner
            .subscribe(topics)
            .map_err(|e| ProviderError::ConsumerError(format!("{:?}", e)))?;
    
        let mut stream = self.inner.stream();
    
        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Ok(kmsg) => {
                    // Convert to OwnedMessage
                    let owned_msg = kmsg.detach();
    
                    // Call handler with OwnedMessage
                    match handler(owned_msg).await {
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