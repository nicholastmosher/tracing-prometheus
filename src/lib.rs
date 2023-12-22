#![warn(clippy::panic)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use prometheus_client::{
    metrics::{
        counter::Counter,
        family::{Family, MetricConstructor},
        gauge::Gauge,
        histogram::Histogram,
        info::Info,
        MetricType,
    },
    registry::Registry,
};
use smallstr::SmallString;
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
    histograms: BTreeMap<&'static str, Family<MetricLabels, Histogram, HistogramBuckets>>,

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
                    MetricSample::Info { meta } => {
                        let metric = Info::new(meta.labels.clone());
                        // SAFETY: No panic possible during any write guard, should not be poisoned
                        #[allow(clippy::expect_used)]
                        let mut state = self.state.write().expect("write metrics state");
                        state.registry.register(meta.name, &meta.help, metric);
                        state.infos.insert(meta.name);
                    }
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
                    MetricSample::Histogram { meta, buckets, .. } => {
                        let buckets = buckets.clone();
                        let metric =
                            Family::<MetricLabels, Histogram, _>::new_with_constructor(buckets);
                        // SAFETY: No panic possible during any write guard, should not be poisoned
                        #[allow(clippy::expect_used)]
                        let mut state = self.state.write().expect("write metrics state");
                        state
                            .registry
                            .register(meta.name, &meta.help, metric.clone());
                        state.histograms.insert(meta.name, metric);
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
                MetricSample::Histogram { meta, value, .. } => {
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
    Info {
        meta: MetricMeta,
    },
    Counter {
        meta: MetricMeta,
        value: u64,
    },
    Gauge {
        meta: MetricMeta,
        value: i64,
    },
    Histogram {
        meta: MetricMeta,
        value: f64,
        buckets: HistogramBuckets,
    },
}

#[derive(Debug)]
struct MetricEventAst {
    kind: MetricType,
    value: Option<MetricField>,
    help: Option<String>,
    labels: MetricLabels,
    buckets: Option<HistogramBuckets>,
}

#[derive(Debug)]
enum MetricField {
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(SmallString<[u8; 64]>),
}

impl MetricField {
    fn str(s: &str) -> Self {
        Self::Str(SmallString::from_str(s))
    }
}

#[derive(Debug, Clone)]
struct HistogramBuckets(Vec<f64>);

impl std::str::FromStr for HistogramBuckets {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let rest = s.strip_prefix('[').ok_or(())?;
        let rest = rest.strip_suffix(']').ok_or(())?;
        let buckets = rest
            .split(',')
            .map(|i| i.trim().parse::<f64>().map_err(|_| ()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self(buckets))
    }
}

impl MetricConstructor<Histogram> for HistogramBuckets {
    fn new_metric(&self) -> Histogram {
        Histogram::new(self.0.clone().into_iter())
    }
}

type MetricLabels = Vec<(&'static str, String)>;

impl MetricEventAst {
    fn new(kind: MetricType) -> Self {
        Self {
            kind,
            value: None,
            help: None,
            labels: Default::default(),
            buckets: None,
        }
    }

    fn set_value(&mut self, value: MetricField) {
        self.value = Some(value);
    }

    fn set_help(&mut self, help: &str) {
        self.help = Some(help.to_string());
    }

    fn set_label(&mut self, name: &'static str, value: &str) {
        self.labels.push((name, value.to_string()));
    }

    fn set_buckets(&mut self, buckets: HistogramBuckets) {
        self.buckets = Some(buckets);
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
                    Some(MetricField::U64(value)) => value,
                    Some(MetricField::I64(value)) => u64::try_from(value).map_err(|_| ())?,
                    _ => return Err(()),
                };
                MetricSample::Counter { meta, value }
            }
            MetricType::Gauge => {
                let value = match self.value {
                    Some(MetricField::U64(value)) => i64::try_from(value).map_err(|_| ())?,
                    Some(MetricField::I64(value)) => value,
                    _ => return Err(()),
                };
                MetricSample::Gauge { meta, value }
            }
            MetricType::Histogram => {
                let value = match self.value {
                    Some(MetricField::F64(value)) => value,
                    _ => return Err(()),
                };
                let buckets = self.buckets.ok_or(())?;
                MetricSample::Histogram {
                    meta,
                    value,
                    buckets,
                }
            }
            MetricType::Info => MetricSample::Info { meta },
            _ => return Err(()),
        };
        Ok(sample)
    }
}

#[derive(Debug)]
struct MetricEventVisitor<'config> {
    prefix: &'config str,
    asts: BTreeMap<&'static str, MetricEventAst>,
}

impl MetricEventVisitor<'_> {
    pub fn new() -> Self {
        Self {
            prefix: "metrics.",
            asts: Default::default(),
        }
    }

    // TODO(nick): Better error
    fn into_samples(self) -> Result<Vec<MetricSample>, ()> {
        self.asts
            .into_iter()
            .map(|(name, builder)| (builder.into_sample(name)))
            .collect::<Result<Vec<_>, _>>()
    }
}

impl<'config> MetricEventVisitor<'config> {
    fn record_field(&mut self, field: &tracing::field::Field, value: MetricField) {
        // Short-circuit if this isn't a metric
        //
        // - metrics.counter.request_count = 1 // Good
        // - http.port = 8080 // Ignored
        let Some(metric) = field.name().strip_prefix(self.prefix) else {
            return;
        };

        let mut pieces = metric.split('.');

        // Split kind and name
        //
        // - metrics.counter.request_count = 1 // Good
        // - metrics.counter // Ignored
        let Some(kind) = pieces.next() else {
            tracing::warn!("Expected metric kind, found {}", field.name());
            return;
        };

        // Parse metric kind
        //
        // - metrics.counter.request_count = 1 // Good
        // - metrics.gauge.request_count = 1 // Good
        // - metrics.histogram.request_count = 1 // Good
        // - metrics.info.request_count = 1 // Good
        // - metrics.goober.request_count = 1 // Ignored
        let Some(kind) = parse_metric_type(kind) else {
            tracing::warn!("Expected metric kind, found {}", field.name());
            return;
        };

        let Some(name) = pieces.next() else {
            tracing::warn!("Missing name in metric field {}, ignoring", field.name());
            return;
        };

        // Find or create an Ast to represent this metric, getting a ref to it
        let ast = self
            .asts
            .entry(name)
            .or_insert_with(|| MetricEventAst::new(kind));

        // Ensure samples to the same metric are written as the same type
        //
        // - metrics.counter.request_count = 1 // Ok
        // - metrics.histogram.request_count = 1 // Bad, request_count was set as counter
        if ast.kind.as_str() != kind.as_str() {
            tracing::warn!(
                "Metric {} is a {}, received {} sample - ignoring",
                name,
                ast.kind.as_str(),
                kind.as_str()
            );
            return;
        }

        // If there are no more pieces to this field, this is the metric's "value"
        //
        // - metrics.counter.request_count = 1 => metric value 1
        let Some(metric_field) = pieces.next() else {
            ast.set_value(value);
            return;
        };

        // Determine what to do based on what the field on the metric is
        //
        // - metrics.counter.request_count.help = "HELP" => help=HELP
        // - metrics.counter.request_count.label.<key> = "thing" => label key=thing
        // - metrics.counter.request_count.exemplar = true => exemplar
        // - metrics.histogram.request_duration.buckets = "[0.1, 0.2]" => buckets
        //

        match (metric_field, kind, &value) {
            ("buckets", MetricType::Histogram, MetricField::Str(s)) => {
                let Ok(buckets) = s.parse::<HistogramBuckets>() else {
                    tracing::warn!("Failed to read buckets {s}, skipping");
                    return;
                };

                ast.set_buckets(buckets);
                return;
            }
            ("buckets", _, _) => {
                tracing::warn!(
                    "'buckets' only applicable for histogram, found {}, ignoring",
                    field.name()
                );
                return;
            }
            _ => {}
        }

        // 'help' must be a string
        match (metric_field, &value) {
            ("help", MetricField::Str(help)) => {
                ast.set_help(help.as_ref());
                return;
            }
            ("help", _) => {
                tracing::warn!("'help' should be string for {}, ignoring", field.name());
                return;
            }
            _ => {}
        }

        // 'exemplar' expects true or false
        match (metric_field, &value) {
            ("exemplar", MetricField::Bool(_)) => {
                ast.set_value(value);
                return;
            }
            ("exemplar", _) => {
                tracing::warn!("'exemplar' should be bool for {}, ignoring", field.name());
                return;
            }
            _ => {}
        }

        // 'label' expects another field segment for the label key
        if metric_field == "label" {
            let Some(label_key) = pieces.next() else {
                tracing::warn!("Missing label key for {}, ignoring", field.name());
                return;
            };

            let MetricField::Str(label_value) = value else {
                tracing::warn!(
                    "'label' expects string value for {}, ignoring",
                    field.name()
                );
                return;
            };

            ast.set_label(label_key, label_value.as_str());
        }
    }
}

impl Visit for MetricEventVisitor<'_> {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.record_field(field, MetricField::Bool(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record_field(field, MetricField::U64(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record_field(field, MetricField::I64(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record_field(field, MetricField::F64(value));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record_field(field, MetricField::str(value));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tracing_subscriber::prelude::*;

    use super::*;

    #[test]
    fn test_split_no_dot() {
        assert_eq!(
            "batch_size".split('.').collect::<Vec<_>>(),
            vec!["batch_size"]
        );
    }

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
            metrics.counter.consumed_batches.exemplar = true,
        );
        tracing::trace!(
            metrics.counter.consumed_batches = 4u64,
            metrics.counter.consumed_batches.help = "This is the number of consumed batches",
            metrics.counter.consumed_batches.label.app_id = "12345",
            metrics.counter.consumed_batches.label.topic = "cdc",
            metrics.counter.consumed_batches.label.group = "webhook",
        );

        tracing::trace!(
            metrics.histogram.batch_size = 4f64,
            metrics.histogram.batch_size.help = "This is the size of the batch",
            metrics.histogram.batch_size.label.app_id = "12345",
            metrics.histogram.batch_size.label.topic = "cdc",
            metrics.histogram.batch_size.label.group = "webhook",
            metrics.histogram.batch_size.buckets = ?exponential_buckets(0.1, 2.0, 10),
        );

        tracing::trace!(
            metrics.info.build.help = "This is the build info of the service",
            metrics.info.build.label.app_id = "12345",
            metrics.info.build.label.topic = "transactions",
            metrics.info.build.label.group = "webhook",
        );

        let rendered = handle.render().unwrap();
        println!("{rendered}");
    }

    fn exponential_buckets(mut initial: f64, factor: f64, count: u32) -> Vec<f64> {
        let mut buckets = vec![initial];
        for _ in 0..count {
            initial *= factor;
            buckets.push(initial);
        }
        buckets
    }
}
