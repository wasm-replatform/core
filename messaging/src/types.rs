use thiserror::Error;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub group_id: Option<String>, // only used for consumer
    pub enable_auto_commit: Option<bool>,
    pub js_partitioner: Option<bool>,
    pub partition_count: Option<i32>, // only used for producer
}

#[derive(Debug, Clone)]
pub struct SchemaConfig {
    pub url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub cache_ttl_secs: Option<u64>,
}

#[derive(Debug)]
pub struct DeliveryInfo {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub headers: Option<Vec<(String, Vec<u8>)>>,
}

#[derive(Error, Debug)]
pub enum MessagingError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("send failed: {0}")]
    SendFailed(String),
    #[error("consumer error: {0}")]
    ConsumerError(String),
    #[error("schema registry error: {0}")]
    SchemaRegistryError(String),
    #[error("other: {0}")]
    Other(String),
}
