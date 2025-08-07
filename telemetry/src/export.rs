//! # Export
//!
//! Exports spans and metrics to an OpenTelemetry collector.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use opentelemetry_otlp::{self as oltp};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::trace::{self, SpanData};
use rand::Rng;
use tokio::sync::Mutex;
use tokio::task::block_in_place;
use tokio::time;

/// Add retry logic to a `[oltp::SpanExporter]`.
#[derive(Debug, Clone)]
pub struct Exporter {
    inner: Arc<Mutex<oltp::SpanExporter>>,
}

impl Exporter {
    pub fn new(exporter: oltp::SpanExporter) -> Self {
        Self { inner: Arc::new(Mutex::new(exporter)) }
    }
}

impl trace::SpanExporter for Exporter {
    async fn export(&self, batch: Vec<SpanData>) -> Result<(), OTelSdkError> {
        let this = self.clone();
        this.inner.lock().await.export(batch.clone()).await
    }

    fn shutdown(&mut self) -> Result<(), OTelSdkError> {
        block_in_place(|| self.inner.blocking_lock().shutdown())
    }

    fn force_flush(&mut self) -> Result<(), OTelSdkError> {
        let this = self.clone();
        block_in_place(|| this.inner.blocking_lock().force_flush())
    }

    fn set_resource(&mut self, resource: &Resource) {
        block_in_place(|| {
            self.inner.blocking_lock().set_resource(resource);
        });
    }
}

/// Add retry logic to a `MetricExporter`.
pub struct MetricExporter {
    inner: oltp::MetricExporter,
}

impl MetricExporter {
    pub const fn new(exporter: oltp::MetricExporter) -> Self {
        Self { inner: exporter }
    }
}

impl PushMetricExporter for MetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> Result<(), OTelSdkError> {
        self.inner.export(metrics).await
    }

    fn force_flush(&self) -> Result<(), OTelSdkError> {
        self.inner.force_flush()
    }

    fn shutdown(&self) -> Result<(), OTelSdkError> {
        self.inner.shutdown()
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }

    fn shutdown_with_timeout(&self, _: Duration) -> Result<(), OTelSdkError> {
        todo!()
    }
}
