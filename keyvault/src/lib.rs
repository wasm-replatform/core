use std::sync::Arc;

use anyhow::{Result, bail};
use azure_identity::DeveloperToolsCredential;
use azure_security_keyvault_secrets::SecretClient;
use tokio::sync::watch;

pub struct SecretRetriever {
    secret_client: Arc<SecretClient>,
}

impl SecretRetriever {
    pub fn new(keyvault: &str) -> Result<SecretRetriever> {
        let credential = DeveloperToolsCredential::new(None)?;
        Ok(SecretRetriever {
            secret_client: Arc::new(SecretClient::new(
                &format!("https://{keyvault}.vault.azure.net/"),
                credential,
                None,
            )?),
        })
    }

    /// Returns a watchable secret. Only returns after the initial secret has been fetched.
    pub async fn watch_secret(&self, secret_name: &str) -> Watcher {
        let secret = fetch_secret_retrying(&self.secret_client, secret_name).await;
        let (tx, mut rx) = watch::channel(secret.clone());
        rx.mark_changed();

        let secret_client = self.secret_client.clone();
        let secret_name = secret_name.to_string();
        tokio::spawn(async move {
            let mut last_secret = secret;
            loop {
                let secret = fetch_secret_retrying(&secret_client, &secret_name).await;
                if secret != last_secret {
                    last_secret = secret.clone();
                    match tx.send(secret) {
                        Ok(()) => {}
                        Err(watch::error::SendError(_)) => {
                            println!("Watcher dropped, stopping watcher thread");
                            break;
                        }
                    }
                }
            }
        });

        Watcher { rx }
    }
}

pub struct Watcher {
    rx: watch::Receiver<String>,
}

impl Watcher {
    /// Waits until a new secret value is available.
    ///
    /// Returns `None` if the watcher has been dropped.
    pub async fn next(&mut self) -> Option<String> {
        self.rx.changed().await.ok()?;
        Some(self.rx.borrow_and_update().to_string())
    }
}

async fn fetch_secret_retrying(secret_client: &SecretClient, secret_name: &str) -> String {
    loop {
        match fetch_secret(secret_client, secret_name).await {
            Ok(secret) => {
                return secret;
            }
            Err(e) => {
                println!("Error fetching secret: {e}. Retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn fetch_secret(secret_client: &SecretClient, secret_name: &str) -> Result<String> {
    let secret = secret_client.get_secret(secret_name, None).await?.into_body().await?;
    match secret.value {
        Some(value) => Ok(value),
        None => bail!("secret value missing"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_watch_secret() -> Result<()> {
        let retriever = SecretRetriever::new("test")?;
        let mut watcher = retriever.watch_secret("test").await;
        assert_eq!(watcher.next().await.unwrap(), "secret");
        Ok(())
    }
}
