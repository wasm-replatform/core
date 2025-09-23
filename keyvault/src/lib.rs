use std::sync::Arc;
use std::time::Duration;

use azure_core::credentials::{Secret, TokenCredential};
use azure_core::http::StatusCode;
use azure_identity::{ClientSecretCredential, DeveloperToolsCredential};
use azure_security_keyvault_secrets::SecretClient;
use tokio::sync::watch;
use tokio::time;

#[derive(Debug)]
pub enum KeyVaultError {
    AzureError(azure_core::Error),
    SecretMissing,
}

impl From<azure_core::Error> for KeyVaultError {
    fn from(error: azure_core::Error) -> Self {
        Self::AzureError(error)
    }
}

pub type Result<T> = std::result::Result<T, KeyVaultError>;

pub struct KeyVaultConfig {
    /// Only the vault name is required, not the full url.
    pub keyvault: String,
    /// Will use developer credentials if not specified.
    pub credentials: Option<CredentialConfig>,
}

pub struct CredentialConfig {
    pub client_id: String,
    pub client_secret: String,
    pub tenant_id: String,
}

pub struct SecretRetriever {
    secret_client: Arc<SecretClient>,
}

impl SecretRetriever {
    /// Create a new `SecretRetriever`.
    pub fn new(config: KeyVaultConfig) -> Result<SecretRetriever> {
        let credential: Arc<dyn TokenCredential> = if let Some(c) = config.credentials {
            let secret = Secret::new(c.client_secret);
            ClientSecretCredential::new(&c.tenant_id, c.client_id, secret, None)?
        } else {
            DeveloperToolsCredential::new(None)?
        };
        Ok(SecretRetriever {
            secret_client: Arc::new(SecretClient::new(
                &format!("https://{}.vault.azure.net/", config.keyvault),
                credential,
                None,
            )?),
        })
    }

    /// Returns a subscriber that can wait for secret changes. Only returns after the initial
    /// secret has been fetched.
    ///
    /// Starts a background watch task that periodically polls for secret updates. If a new,
    /// different secret is found, the subscriber will be notified.
    pub async fn watch_secret(&self, secret_name: &str) -> Result<Subscriber> {
        let secret = fetch_secret_retrying(&self.secret_client, secret_name).await?;
        let (tx, mut rx) = watch::channel(secret.clone());
        rx.mark_changed();

        let secret_client = self.secret_client.clone();
        let secret_name = secret_name.to_string();

        let watcher_tx = tx.clone();
        tokio::spawn(async move {
            let mut last_secret = secret;
            let mut interval = time::interval(Duration::from_hours(1));
            loop {
                interval.tick().await;
                let secret = match fetch_secret_retrying(&secret_client, &secret_name).await {
                    Ok(secret) => secret,
                    Err(error) => {
                        tracing::error!(
                            ?error,
                            "Unrecoverable secret fetching error, stopping watcher task"
                        );
                        break;
                    }
                };
                if secret != last_secret {
                    last_secret = secret.clone();
                    match watcher_tx.send(secret) {
                        Ok(()) => {}
                        Err(watch::error::SendError(_)) => {
                            tracing::error!("Watch subscriber dropped, stopping watcher thread");
                            break;
                        }
                    }
                }
            }
        });

        Ok(Subscriber { tx, rx })
    }
}

pub struct Subscriber {
    /// Only used for generating new subscribers when cloning.
    tx: watch::Sender<String>,
    rx: watch::Receiver<String>,
}

impl Clone for Subscriber {
    fn clone(&self) -> Self {
        Subscriber { tx: self.tx.clone(), rx: self.tx.subscribe() }
    }
}

impl Subscriber {
    /// Waits until a new secret value is available and returns it.
    ///
    /// Returns `None` if the watcher thread has been stopped.
    pub async fn next(&mut self) -> Option<String> {
        self.rx.changed().await.ok()?;
        Some(self.rx.borrow_and_update().to_string())
    }
}

/// Retries intermittent errors, but fails with unrecoverable errors like access denied.
async fn fetch_secret_retrying(secret_client: &SecretClient, secret_name: &str) -> Result<String> {
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        interval.tick().await;
        match fetch_secret(secret_client, secret_name).await {
            Ok(Some(secret)) => {
                return Ok(secret);
            }
            Ok(None) => {
                return Err(KeyVaultError::SecretMissing);
            }
            Err(e) => match e.kind() {
                azure_core::error::ErrorKind::HttpResponse {
                    status: StatusCode::Forbidden,
                    error_code: _,
                    raw_response: _,
                } => {
                    return Err(e)?;
                }
                _ => {
                    tracing::warn!("Error fetching secret: {e:#?}. Retrying");
                    interval.tick().await;
                }
            },
        }
    }
}

async fn fetch_secret(
    secret_client: &SecretClient, secret_name: &str,
) -> azure_core::Result<Option<String>> {
    let secret = secret_client.get_secret(secret_name, None).await?.into_body().await?;
    Ok(secret.value)
}
