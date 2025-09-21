use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time;

use serde_json::Value;
use jsonschema::validate;

use schema_registry_client::rest::schema_registry_client::{Client, SchemaRegistryClient};
use schema_registry_client::rest::models::RegisteredSchema;
use schema_registry_client::rest::client_config::{BasicAuth, ClientConfig as SchemaClientConfig};

use crate::{SchemaConfig, MessagingError};

/// Decoded Kafka message
pub struct DecodedPayload<'a> {
    pub magic_byte: u8,
    pub registry_id: u32,
    pub payload: &'a [u8],
}

impl DecodedPayload<'_> {
    /// Encode payload with schema registry ID repeats JS code
    pub fn encode(registry_id: u32, payload: Vec<u8>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4 + payload.len());

        // Magic byte
        buf.push(MAGIC_BYTE);

        // Registry ID in big-endian
        buf.extend(&registry_id.to_be_bytes());

        // Payload
        buf.extend(payload);

        buf
    }

    /// Decode payload
    pub fn decode(buffer: &[u8]) -> Result<DecodedPayload<'_>, MessagingError> {
        if buffer.len() < 5 {
            return Err(MessagingError::SchemaRegistryError(
                "Buffer too short to decode".to_string(),
            ));
        }
    
        let magic_byte = buffer[0];
        let registry_id = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
        let payload = &buffer[5..];
    
        Ok(DecodedPayload {
            magic_byte,
            registry_id,
            payload,
        })
    }
}

pub struct SRClient {
    client: Option<SchemaRegistryClient>,
    schemas: Arc<Mutex<HashMap<String, RegisteredSchema>>>,
}

/// Constants for encoding/decoding
const MAGIC_BYTE: u8 = 0; // single byte

impl SRClient {
    pub fn new(schema_cfg: SchemaConfig) -> Self {
        // Build optional basic auth
        let auth: Option<BasicAuth> = schema_cfg.api_key.as_ref().map(|key| {
            (key.clone(), schema_cfg.api_secret.clone()) // BasicAuth = (String, Option<String>)
        });
    
        // Create SchemaRegistry client config with just URLs
        let mut client_config = SchemaClientConfig::new(vec![schema_cfg.url.clone()]);
    
        // Set basic auth if present
        if let Some((username, password)) = auth {
            client_config.basic_auth = Some((username, password));
        }
    
        // Create the schema registry client
        let client = Some(SchemaRegistryClient::new(client_config));
    
        let schemas = Arc::new(Mutex::new(HashMap::new()));
    
        let sr_client = Self {
            client,
            schemas: Arc::clone(&schemas),
        };
    
        // Start background cache cleaner only if TTL is provided
        if let Some(ttl_secs) = schema_cfg.cache_ttl_secs {
            sr_client.start_cache_cleaner(ttl_secs);
        }
    
        sr_client
    }
   
    /// Serialize payload to JSON with optional schema registry
    pub async fn validate_and_encode_json(
        &self,
        topic: &str,
        buffer: Vec<u8>,
    ) -> Result<Vec<u8>, MessagingError> {
        // If schema registry is available, use it
        if self.client.is_some() {
            let schema = self.get_or_fetch_schema(topic).await?;
            let payload: Value = serde_json::from_slice(&buffer)
                .map_err(|e| MessagingError::SchemaRegistryError(format!("Invalid JSON: {:?}", e)))?;

            // Validate payload against schema
            self.validate_payload_with_schema(&schema, &payload)?;

            // Get the registry ID, return error if missing
            let registry_id = schema.id.ok_or_else(|| {
                MessagingError::SchemaRegistryError(format!("Registry ID for topic {} is missing", topic))
            })? as u32;

            // Kafka encoding with magic byte + registry ID
            Ok(DecodedPayload::encode(registry_id, buffer))
        } else {
            // No schema registry → fallback to plain JSON
            serde_json::to_vec(&buffer).map_err(|e| {
                MessagingError::SchemaRegistryError(format!("JSON serialization failed: {:?}", e))
            })
        }
    }

    /// Deserialize payload to JSON with optional schema registry
    pub async fn validate_and_decode_json(
        &self,
        topic: &str,
        buffer: &[u8],
    ) -> Result<Vec<u8>, MessagingError> {
        if self.client.is_some() {
            let message = DecodedPayload::decode(buffer)?;
    
            // Fetch the schema for this topic (optional cache)
            let schema = self.get_or_fetch_schema(topic).await?;

            let payload: Value = serde_json::from_slice(&message.payload)
                .map_err(|e| MessagingError::SchemaRegistryError(format!("Invalid JSON: {:?}", e)))?;

            self.validate_payload_with_schema(&schema, &payload)?;
            
            Ok(message.payload.to_vec())
        } else {
            // No schema registry → plain JSON
            serde_json::from_slice(buffer).map_err(|e| {
                MessagingError::SchemaRegistryError(format!("JSON deserialization failed: {:?}", e))
            })
        }
    }

    /// Validate a JSON payload against a provided RegisteredSchema
    pub fn validate_payload_with_schema(
        &self,
        schema: &RegisteredSchema,
        payload: &Value,
    ) -> Result<(), MessagingError> {
        let schema_str = schema.schema.as_ref().ok_or_else(|| {
            MessagingError::SchemaRegistryError("Schema string is missing".into())
        })?;

        let schema_json: Value = serde_json::from_str(schema_str)
            .map_err(|e| MessagingError::SchemaRegistryError(format!("Invalid schema JSON: {:?}", e)))?;

        // Simple one-off validation
        validate(&schema_json, payload).map_err(|e| {
            MessagingError::SchemaRegistryError(format!("JSON validation failed: {}", e))
        })?;

        Ok(())
    }

    async fn get_or_fetch_schema(&self, topic: &str) -> Result<RegisteredSchema, MessagingError> {
        // If no schema registry client, return error (caller can handle fallback)
        let sr = self.client.as_ref().ok_or_else(|| {
            MessagingError::SchemaRegistryError("No schema registry client available".to_string())
        })?;

        // Lock the schema cache
        let mut schemas = self.schemas.lock().await;
        if let Some(schema) = schemas.get(topic) {
            Ok(schema.clone())
        } else {
            // Fetch from registry
            let subject = format!("{}-value", topic);
            let schema_response = sr
                .get_latest_version(&subject, None)
                .await
                .map_err(|e| {
                    MessagingError::SchemaRegistryError(format!(
                        "Failed to fetch schema for {}: {:?}",
                        subject, e
                    ))
                })?;            

            // Cache it
            schemas.insert(topic.to_string(), schema_response.clone());
            Ok(schema_response)
        }
    }

    /// Private method to spawn the cache cleaner task every hour
    fn start_cache_cleaner(&self, cache_ttl_secs: u64) {
        let schemas_clone = Arc::clone(&self.schemas);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(cache_ttl_secs));
            loop {
                interval.tick().await;
                let mut map = schemas_clone.lock().await;
                map.clear();
                tracing::info!("[SRClient] Schema cache cleared");
            }
        });
    }
}