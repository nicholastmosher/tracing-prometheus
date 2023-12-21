use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use prometheus_client::{
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram, MetricType},
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
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct Descriptor {
    name: Cow<'static, str>,
    help: Cow<'static, str>,
}

impl<S> tracing_subscriber::Layer<S> for MetricsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        let is_metric = metadata
            .fields()
            .iter()
            .any(|field| field.name().starts_with("metrics."));
        if !is_metric {
            return tracing::subscriber::Interest::never();
        }

        // All fields beginning with `.metrics.<kind>`, sorted by kind
        let metric_fields_by_kind = metadata
            .fields()
            .iter()
            .filter_map(|field| field.name().strip_prefix("metrics."))
            .filter_map(|metric| metric.split_once('.'))
            .filter_map(|(kind, name_etc)| parse_metric_type(kind).map(|kind| (kind, name_etc)))
            .collect::<Vec<_>>();

        // Names of all counters, gauges, and histograms
        let (counters, gauges, histograms) = metric_fields_by_kind
            .iter()
            .filter(|(_kind, name_etc)| !name_etc.contains('.'))
            .fold(
                Default::default(),
                |acc: (Vec<_>, Vec<_>, Vec<_>), (kind, name)| {
                    let (mut counters, mut gauges, mut histograms) = acc;
                    match kind {
                        MetricType::Counter => counters.push(name),
                        MetricType::Gauge => gauges.push(name),
                        MetricType::Histogram => histograms.push(name),
                        _ => {}
                    }
                    (counters, gauges, histograms)
                },
            );

        tracing::subscriber::Interest::always()
    }

    fn on_record(
        &self,
        _span: &tracing::span::Id,
        _values: &tracing::span::Record<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
    }

    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = MetricEventVisitor::new();
        event.record(&mut visitor);
        // println!("{visitor:#?}");

        let new_metrics = {
            let state = self.state.read().expect("read metrics state");
            visitor
                .samples
                .iter()
                .filter(|(name, _event)| {
                    if state.counters.contains_key(*name) {
                        return false;
                    }
                    if state.gauges.contains_key(*name) {
                        return false;
                    }
                    if state.histograms.contains_key(*name) {
                        return false;
                    }
                    true
                })
                .collect::<Vec<_>>()
        };

        // New metrics need to be registered
        if !new_metrics.is_empty() {
            for (name, event) in new_metrics {
                let help = event
                    .help
                    .clone()
                    .unwrap_or_else(|| format!("Add help with 'metrics.<type>.{name}.help"));
                match event.kind {
                    MetricType::Counter => {
                        let metric = Family::<MetricLabels, Counter>::default();
                        let mut state = self.state.write().expect("write metrics state");
                        state.registry.register(*name, help, metric.clone());
                        state.counters.insert(name, metric);
                    }
                    MetricType::Gauge => {
                        let metric = Family::<MetricLabels, Gauge>::default();
                        let mut state = self.state.write().expect("write metrics state");
                        state.registry.register(*name, help, metric.clone());
                        state.gauges.insert(name, metric);
                    }
                    MetricType::Histogram => {
                        let metric =
                            Family::<MetricLabels, Histogram>::new_with_constructor(|| {
                                // TODO(nick): Figure out how to configure this
                                Histogram::new([0.1_f64].into_iter())
                            });
                        let mut state = self.state.write().expect("write metrics state");
                        state.registry.register(*name, help, metric.clone());
                        state.histograms.insert(name, metric);
                    }
                    unsupported => {
                        tracing::warn!("Ignoring unsupported metric type {unsupported:?}");
                    }
                }
            }
        }

        // Record actual values
        for (name, event) in visitor.samples {
            match event.kind {
                MetricType::Counter => {
                    // TODO(nick): Make this infallible
                    let Some(MetricValue::Counter(value)) = event.value else {
                        panic!("expected counter")
                    };
                    let state = self.state.read().expect("read metrics state");
                    state
                        .counters
                        .get(name)
                        .expect("should have counter")
                        .get_or_create(&event.labels)
                        .inc_by(value); // TODO(nick): Use real value
                }
                MetricType::Gauge => {
                    // TODO(nick): Make this infallible
                    let Some(MetricValue::Gauge(value)) = event.value else {
                        panic!("expected gauge")
                    };
                    let state = self.state.read().expect("read metrics state");
                    state
                        .gauges
                        .get(name)
                        .expect("should have gauge")
                        .get_or_create(&event.labels)
                        .set(value); // TODO(nick): Use real value
                }
                MetricType::Histogram => {
                    // TODO(nick): Make this infallible
                    let Some(MetricValue::Histogram(value)) = event.value else {
                        panic!("expected histogram value")
                    };
                    let state = self.state.read().expect("read metrics state");
                    state
                        .histograms
                        .get(name)
                        .expect("should have histogram")
                        .get_or_create(&event.labels)
                        .observe(value);
                }
                unsupported => {
                    tracing::warn!("")
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
        _ => return None,
    };
    Some(kind)
}

type MetricLabels = Vec<(&'static str, String)>;

#[derive(Debug)]
enum MetricValue {
    Counter(u64),
    Gauge(i64),
    Histogram(f64),
}

#[derive(Debug)]
struct MetricEvent {
    kind: MetricType,
    // TODO(nick): Support f64 or u64
    value: Option<MetricValue>,
    help: Option<String>,
    labels: MetricLabels,
}

impl MetricEvent {
    pub fn new(kind: MetricType) -> Self {
        Self {
            kind,
            value: None,
            help: None,
            labels: Default::default(),
        }
    }

    pub fn set_value(&mut self, value: MetricValue) {
        self.value = Some(value);
    }

    pub fn set_help(&mut self, help: &str) {
        self.help = Some(help.to_string());
    }

    pub fn set_label(&mut self, name: &'static str, value: &str) {
        self.labels.push((name, value.to_string()));
    }
}

#[derive(Debug)]
struct MetricEventVisitor {
    samples: BTreeMap<&'static str, MetricEvent>,
}

impl MetricEventVisitor {
    pub fn new() -> Self {
        Self {
            samples: Default::default(),
        }
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

        let sample = self
            .samples
            .entry(name)
            .or_insert_with(|| MetricEvent::new(kind));

        // Ensure samples to the same metric are written as the same type
        //
        // - metrics.counter.request_count = 1 // Ok
        // - metrics.histogram.request_count = 1 // Bad, request_count was set as counter
        if sample.kind.as_str() != kind.as_str() {
            tracing::warn!(
                "Metric {} is a {}, received {} sample - ignoring",
                name,
                sample.kind.as_str(),
                kind.as_str()
            );
            return;
        }

        sample.set_value(value);
    }
}

impl Visit for MetricEventVisitor {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record_metric(field, MetricValue::Counter(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record_metric(field, MetricValue::Gauge(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record_metric(field, MetricValue::Histogram(value));
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
            tracing::warn!("Found string, expected f64 for value {}", field.name());
            return;
        };

        let sample = self
            .samples
            .entry(name)
            .or_insert_with(|| MetricEvent::new(kind));

        // Ensure samples to the same metric are written as the same type
        //
        // - metrics.counter.request_count.help = "Here you go" // Ok
        // - metrics.histogram.request_count.help = "Here you go" // Bad, request_count was set as counter
        if sample.kind.as_str() != kind.as_str() {
            tracing::warn!(
                "Metric {} is a {}, received {} sample - ignoring",
                name,
                sample.kind.as_str(),
                kind.as_str()
            );
            return;
        }

        if rest == "help" {
            sample.set_help(value);
            return;
        }

        if let Some(label_name) = rest.strip_prefix("label.") {
            sample.set_label(label_name, value);
            return;
        }
    }
}

#[cfg(test)]
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

        let rendered = handle.render().unwrap();
        println!("{rendered}");
    }
}
