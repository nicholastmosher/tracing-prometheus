# `tracing-prometheus`

> This crate is experimental and not intended for use in production

This crate is an experiment for seamlessly capturing metrics data using
the `tracing` family of macros. The goal is to enable something like the following:

```rust
let batch_size = 50_u64,
tracing::trace!(
    // Metric names and values encoded as `metrics.<type>.<name> = <value>`
    metrics.histogram.batch_size = batch_size,

    // Metric fields encoded as `<name>.<field>`
    metrics.histogram.batch_size.help = "Message count in a Kafka batch",

    // Key-value labels encoded as .label.<label_key> = "<label-value>"
    metrics.histogram.batch_size.label.topic = "my-topic",

    // Goal: Use boolean to decide whether to record exemplar
    metrics.histogram.batch_size.exemplar = batch_size > 100,

    // Emit standard traces/logs from the same call
    "Consumed batch from Kafka",
);
```

Here's a quick list of ideas/goals/non-goals that I have about the future of
this crate:

- The metrics DSL/API should be prometheus-first, rather than trying to be
  fully general to any metrics system. Hence the name `tracing-prometheus`

  - Currently I'm using the official `prometheus-client` crate, since it's the
    only one I know of that natively supports exemplars, which is a goal of
    this crate.

- Experimental goal: It should be possible to use a single tracing call to
  instrument multiple metrics as well as emit a trace with arbitrary fields
  like normal.

- Experimental goal: It should be possible to automatically capture exemplars
  between metrics and traces. In this library, since metrics are captured with
  the same instrumentation as traces, it should be natural to attach tracing
  context to metrics samples.

- Goal: It should be possible to integrate with the `opentelemetry` ecosystem
  to automatically associate metrics recordings with distributed traces.

# Related projects / Prior art

- [`tracing-opentelemetry`] has a `MetricsLayer` that allows similar metrics
  instrumentation

[`tracing-opentelemetry`]: https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/struct.MetricsLayer.html
