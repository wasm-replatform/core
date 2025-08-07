//! # Telemtry
//!
//! Telemetry is a module that provides functionality for collecting and
//! reporting OpenTelemetry-based metrics.

mod export;
mod init;
mod tracing;

use anyhow::Result;
pub use tracing::*;

/// Telemetry initializer.
#[derive(Debug, Default)]
pub struct Otel {
    /// The name of the application to for the purposes of identifying the
    /// service in telemetry data.
    app_name: String,

    /// The name of the environment, e.g. "production", "staging", "development".
    env_name: Option<String>,

    /// The OpenTelemetry metrics collection endpoint.
    endpoint: Option<String>,
}

impl Otel {
    /// Create a new telemetry initializer.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { app_name: name.into(), env_name: None, endpoint: None }
    }

    /// Override the default app name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.app_name = name.into();
        self
    }

    /// Set the environment name.
    #[must_use]
    pub fn env(mut self, env_name: impl Into<String>) -> Self {
        self.env_name = Some(env_name.into());
        self
    }

    /// Set the OpenTelemetry endpoint.
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Initialize telemetry with the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the telemetry system fails to initialize, such as if
    /// the OpenTelemetry exporter cannot be created or if setting the global
    /// subscriber fails.
    pub fn init(self) -> Result<()> {
        init::init(&init::Config {
            app_name: self.app_name,
            env_name: self.env_name,
            endpoint: self.endpoint,
        })
    }
}
