#![warn(clippy::panic)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use prometheus_client::{
    metrics::{
        counter::Counter, family::Family, gauge::Gauge, histogram::Histogram, info::Info,
        MetricType,
    },
    registry::Registry,
};
use tracing::{field::Visit, Subscriber};
use tracing_subscriber::registry::LookupSpan;

#[derive(Default, Clone)]
pub struct MetricsLayer {
    state: Arc<std::sync::RwLock<MetricsLayerState>>,
}

impl MetricsLayer {
    pub fn render(&self) -> Result<String, std::fmt::Error> {
        // SAFETY: RwLock should not be poisoned, if so it's a bug
        #[allow(clippy::expect_used)]
        let state = self.state.read().expect("read metrics state");
        let mut rendered = String::new();
        prometheus_client::encoding::text::encode(&mut rendered, &state.registry)?;
        Ok(rendered)
    }
}

#[derive(Default)]
pub struct MetricsLayerState {
    /// Prometheus registry
    registry: Registry,

    /// Handles to counters
    counters: BTreeMap<&'static str, Family<MetricLabels, Counter>>,

    /// Handles to gauges
    gauges: BTreeMap<&'static str, Family<MetricLabels, Gauge>>,

    /// Handles to histograms
    histograms: BTreeMap<&'static str, Family<MetricLabels, Histogram>>,

    /// Set of infos that have been registered
    infos: BTreeSet<&'static str>,
}

impl<S> tracing_subscriber::Layer<S> for MetricsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = MetricEventVisitor::new();
        event.record(&mut visitor);
        let Ok(samples) = visitor.into_samples() else {
            tracing::warn!("Error converting samples");
            return;
        };

        let new_metrics = {
            // SAFETY: No panic possible during any write guard, should not be poisoned
            #[allow(clippy::expect_used)]
            let state = self.state.read().expect("read metrics state");
            samples
                .iter()
                .filter(|sample| match sample {
                    MetricSample::Counter { meta, .. } => !state.counters.contains_key(meta.name),
                    MetricSample::Gauge { meta, .. } => !state.gauges.contains_key(meta.name),
                    MetricSample::Histogram { meta, .. } => {
                        !state.histograms.contains_key(meta.name)
                    }
                    MetricSample::Info { meta } => !state.infos.contains(meta.name),
                })
                .collect::<Vec<_>>()
        };

        // New metrics need to be registered
        if !new_metrics.is_empty() {
            for sample in new_metrics {
                match sample {
                    MetricSample::Counter { meta, .. } => {
                        let metric = Family::<MetricLabels, Counter>::default();
                        // SAFETY: No panic possible during any write guard, should not be poisoned
                        #[allow(clippy::expect_used)]
                        let mut state = self.state.write().expect("write metrics state");
                        state
                            .registry
                            .register(meta.name, &meta.help, metric.clone());
                        state.counters.insert(meta.name, metric);
                    }
                    MetricSample::Gauge { meta, .. } => {
                        let metric = Family::<MetricLabels, Gauge>::default();
                        // SAFETY: No panic possible during any write guard, should not be poisoned
                        #[allow(clippy::expect_used)]
                        let mut state = self.state.write().expect("write metrics state");
                        state
                            .registry
                            .register(meta.name, &meta.help, metric.clone());
                        state.gauges.insert(meta.name, metric);
                    }
                    MetricSample::Histogram { meta, .. } => {
                        let metric =
                            Family::<MetricLabels, Histogram>::new_with_constructor(|| {
                                // TODO(nick): Figure out how to configure this
                                Histogram::new([0.1_f64].into_iter())
                            });
                        // SAFETY: No panic possible during any write guard, should not be poisoned
                        #[allow(clippy::expect_used)]
                        let mut state = self.state.write().expect("write metrics state");
                        state
                            .registry
                            .register(meta.name, &meta.help, metric.clone());
                        state.histograms.insert(meta.name, metric);
                    }
                    MetricSample::Info { meta } => {
                        let metric = Info::new(meta.labels.clone());
                        // SAFETY: No panic possible during any write guard, should not be poisoned
                        #[allow(clippy::expect_used)]
                        let mut state = self.state.write().expect("write metrics state");
                        state.registry.register(meta.name, &meta.help, metric);
                        state.infos.insert(meta.name);
                    }
                }
            }
        }

        // Record actual values
        for sample in samples {
            match sample {
                MetricSample::Counter { meta, value } => {
                    // SAFETY: No panic possible during any write guard, should not be poisoned
                    #[allow(clippy::expect_used)]
                    let state = self.state.read().expect("read metrics state");

                    // SAFETY: We just checked that each sample either existed or was created
                    #[allow(clippy::expect_used)]
                    state
                        .counters
                        .get(meta.name)
                        .expect("should have counter")
                        .get_or_create(&meta.labels)
                        .inc_by(value);
                }
                MetricSample::Gauge { meta, value } => {
                    // SAFETY: No panic possible during any write guard, should not be poisoned
                    #[allow(clippy::expect_used)]
                    let state = self.state.read().expect("read metrics state");

                    // SAFETY: We just checked that each sample either existed or was created
                    #[allow(clippy::expect_used)]
                    state
                        .gauges
                        .get(meta.name)
                        .expect("should have gauge")
                        .get_or_create(&meta.labels)
                        .set(value);
                }
                MetricSample::Histogram { meta, value } => {
                    // SAFETY: No panic possible during any write guard, should not be poisoned
                    #[allow(clippy::expect_used)]
                    let state = self.state.read().expect("read metrics state");

                    // SAFETY: We just checked that each sample either existed or was created
                    #[allow(clippy::expect_used)]
                    state
                        .histograms
                        .get(meta.name)
                        .expect("should have histogram")
                        .get_or_create(&meta.labels)
                        .observe(value);
                }
                MetricSample::Info { .. } => {
                    // Info only needs registered, it does not record values
                }
            }
        }
    }
}

fn parse_metric_type(s: &str) -> Option<MetricType> {
    let kind = match s {
        "counter" => MetricType::Counter,
        "gauge" => MetricType::Gauge,
        "histogram" => MetricType::Histogram,
        "info" => MetricType::Info,
        _ => return None,
    };
    Some(kind)
}

#[derive(Debug)]
struct MetricMeta {
    name: &'static str,
    help: String,
    labels: MetricLabels,
}

#[derive(Debug)]
enum MetricSample {
    Counter { meta: MetricMeta, value: u64 },
    Gauge { meta: MetricMeta, value: i64 },
    Histogram { meta: MetricMeta, value: f64 },
    Info { meta: MetricMeta },
}

#[derive(Debug)]
struct MetricEventBuilder {
    kind: MetricType,
    value: Option<MetricValue>,
    help: Option<String>,
    labels: MetricLabels,
}

#[derive(Debug)]
enum MetricValue {
    U64(u64),
    I64(i64),
    F64(f64),
}

type MetricLabels = Vec<(&'static str, String)>;

impl MetricEventBuilder {
    fn new(kind: MetricType) -> Self {
        Self {
            kind,
            value: None,
            help: None,
            labels: Default::default(),
        }
    }

    fn set_value(&mut self, value: MetricValue) {
        self.value = Some(value);
    }

    fn set_help(&mut self, help: &str) {
        self.help = Some(help.to_string());
    }

    fn set_label(&mut self, name: &'static str, value: &str) {
        self.labels.push((name, value.to_string()));
    }

    // TODO(nick): Better errors
    fn into_sample(self, name: &'static str) -> Result<MetricSample, ()> {
        let help = self
            .help
            .unwrap_or_else(|| format!("Add help with 'metrics.<type>.{name}.help'"));
        let meta = MetricMeta {
            name,
            help,
            labels: self.labels,
        };
        let sample = match self.kind {
            MetricType::Counter => {
                let value = match self.value {
                    Some(MetricValue::U64(value)) => value,
                    Some(MetricValue::I64(value)) => u64::try_from(value).map_err(|_| ())?,
                    _ => return Err(()),
                };
                MetricSample::Counter { meta, value }
            }
            MetricType::Gauge => {
                let value = match self.value {
                    Some(MetricValue::U64(value)) => i64::try_from(value).map_err(|_| ())?,
                    Some(MetricValue::I64(value)) => value,
                    _ => return Err(()),
                };
                MetricSample::Gauge { meta, value }
            }
            MetricType::Histogram => {
                let value = match self.value {
                    Some(MetricValue::F64(value)) => value,
                    _ => return Err(()),
                };
                MetricSample::Histogram { meta, value }
            }
            MetricType::Info => MetricSample::Info { meta },
            _ => return Err(()),
        };
        Ok(sample)
    }
}

#[derive(Debug)]
struct MetricEventVisitor {
    builders: BTreeMap<&'static str, MetricEventBuilder>,
}

impl MetricEventVisitor {
    pub fn new() -> Self {
        Self {
            builders: Default::default(),
        }
    }

    // TODO(nick): Better error
    fn into_samples(self) -> Result<Vec<MetricSample>, ()> {
        self.builders
            .into_iter()
            .map(|(name, builder)| (builder.into_sample(name)))
            .collect::<Result<Vec<_>, _>>()
    }
}

impl MetricEventVisitor {
    fn record_metric(&mut self, field: &tracing::field::Field, value: MetricValue) {
        // Short-circuit if this isn't a metric
        //
        // - metrics.counter.request_count = 1 // Good
        // - http.port = 8080 // Ignored
        let Some(metric) = field.name().strip_prefix("metrics.") else {
            return;
        };

        // Split kind and name
        //
        // - metrics.counter.request_count = 1 // Good
        // - metrics.counter // Ignored
        let Some((kind, name)) = metric.split_once('.') else {
            tracing::warn!("Expected metric kind and name, found {}", field.name());
            return;
        };

        // Parse metric kind
        //
        // - metrics.counter.request_count = 1 // Good
        // - metrics.gauge.request_count = 1 // Good
        // - metrics.histogram.request_count = 1 // Good
        // - metrics.goober.request_count = 1 // Ignored
        let Some(kind) = parse_metric_type(kind) else {
            tracing::warn!("Expected metric kind, found {}", field.name());
            return;
        };

        // When visiting f64, value should live at the end of the name
        //
        // - metrics.counter.request_count = 1 // Good
        // - metrics.counter.request_count.value = 1 // Ignored
        // - metrics.counter.request_count.help = "Blah" // Ignored
        // - metrics.counter.request_count.label.anything = "Blah" // Ignored
        if name.contains('.') {
            tracing::warn!("Found f64, expected string for field {}", field.name());
            return;
        }

        let builder = self
            .builders
            .entry(name)
            .or_insert_with(|| MetricEventBuilder::new(kind));

        // Ensure samples to the same metric are written as the same type
        //
        // - metrics.counter.request_count = 1 // Ok
        // - metrics.histogram.request_count = 1 // Bad, request_count was set as counter
        if builder.kind.as_str() != kind.as_str() {
            tracing::warn!(
                "Metric {} is a {}, received {} sample - ignoring",
                name,
                builder.kind.as_str(),
                kind.as_str()
            );
            return;
        }

        builder.set_value(value);
    }
}

impl Visit for MetricEventVisitor {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record_metric(field, MetricValue::U64(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record_metric(field, MetricValue::I64(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record_metric(field, MetricValue::F64(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        // Short-circuit if this isn't a metric
        //
        // - metrics.counter.request_count.help = "Here you go" // Good
        // - http.port = 8080 // Ignored
        let Some(metric) = field.name().strip_prefix("metrics.") else {
            return;
        };

        // Split kind and name
        //
        // - metrics.counter.request_count.help = "Here you go" // Good
        // - metrics.counter // Ignored
        let Some((kind, name_etc)) = metric.split_once('.') else {
            tracing::warn!("Expected metric kind and name, found {}", field.name());
            return;
        };

        // Parse metric kind
        //
        // - metrics.counter.request_count.help = "Here you go" // Good
        // - metrics.gauge.request_count.help = "Here you go" // Good
        // - metrics.histogram.request_count.help = "Here you go" // Good
        // - metrics.goober.request_count.help = "Here you go" // Ignored
        let Some(kind) = parse_metric_type(kind) else {
            tracing::warn!("Expected metric kind, found {}", field.name());
            return;
        };

        // When visiting string, value should live on a field after the name
        //
        // - metrics.counter.request_count.help = "Here you go" // Good
        // - metrics.counter.request_count.label.method = "GET" // Good
        // - metrics.counter.request_count = 1 // Ignored
        let Some((name, rest)) = name_etc.split_once('.') else {
            tracing::warn!("Found string, expected number for value {}", field.name());
            return;
        };

        let builder = self
            .builders
            .entry(name)
            .or_insert_with(|| MetricEventBuilder::new(kind));

        // Ensure samples to the same metric are written as the same type
        //
        // - metrics.counter.request_count.help = "Here you go" // Ok
        // - metrics.histogram.request_count.help = "Here you go" // Bad, request_count was set as counter
        if builder.kind.as_str() != kind.as_str() {
            tracing::warn!(
                "Metric {} is a {}, received {} sample - ignoring",
                name,
                builder.kind.as_str(),
                kind.as_str()
            );
            return;
        }

        if rest == "help" {
            builder.set_help(value);
            return;
        }

        if let Some(label_name) = rest.strip_prefix("label.") {
            builder.set_label(label_name, value);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tracing_subscriber::prelude::*;

    use super::*;

    #[test]
    fn test_tracing_metrics_api() {
        let layer = MetricsLayer::default();
        let handle = layer.clone();
        tracing_subscriber::registry().with(layer).init();

        tracing::trace!(
            metrics.counter.consumed_batches = 1u64,
            metrics.counter.consumed_batches.help = "This is the number of consumed batches",
            metrics.counter.consumed_batches.label.app_id = "12345",
            metrics.counter.consumed_batches.label.topic = "cdc",
            metrics.counter.consumed_batches.label.group = "webhook",
        );
        tracing::trace!(
            metrics.counter.consumed_batches = 4u64,
            metrics.counter.consumed_batches.help = "This is the number of consumed batches",
            metrics.counter.consumed_batches.label.app_id = "12345",
            metrics.counter.consumed_batches.label.topic = "cdc",
            metrics.counter.consumed_batches.label.group = "webhook",
        );

        tracing::trace!(
            metrics.counter.consumed_batches = 2u64,
            metrics.counter.consumed_batches.help = "This is the number of consumed batches",
            metrics.counter.consumed_batches.label.app_id = "12345",
            metrics.counter.consumed_batches.label.topic = "transactions",
            metrics.counter.consumed_batches.label.group = "webhook",
        );

        tracing::trace!(
            metrics.info.build.help = "This is the number of consumed batches",
            metrics.info.build.label.app_id = "12345",
            metrics.info.build.label.topic = "transactions",
            metrics.info.build.label.group = "webhook",
        );

        let rendered = handle.render().unwrap();
        println!("{rendered}");
    }
}
