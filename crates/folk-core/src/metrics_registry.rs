//! Concrete `MetricsRegistry` over the `prometheus` crate.

use std::sync::Arc;

use folk_api::{
    Counter as ApiCounter, CounterVec as ApiCounterVec, Gauge as ApiGauge, GaugeVec as ApiGaugeVec,
    Histogram as ApiHistogram, HistogramVec as ApiHistogramVec, MetricsRegistry,
};
use prometheus::{
    CounterVec as PromCounterVec, Encoder, GaugeVec as PromGaugeVec, HistogramOpts,
    HistogramVec as PromHistogramVec, Opts, Registry, TextEncoder,
};
use tracing::warn;

pub struct MetricsRegistryImpl {
    registry: Registry,
}

impl MetricsRegistryImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            registry: Registry::new(),
        })
    }
}

impl MetricsRegistry for MetricsRegistryImpl {
    fn counter_vec(&self, name: &str, help: &str, label_keys: &[&str]) -> Arc<dyn ApiCounterVec> {
        let opts = Opts::new(name, help);
        let cv = PromCounterVec::new(opts, label_keys).expect("valid counter spec");
        if let Err(e) = self.registry.register(Box::new(cv.clone())) {
            warn!(metric = name, error = %e, "duplicate metric registration; this counter will not appear in /metrics output");
        }
        Arc::new(CounterVecAdapter { inner: cv })
    }

    fn gauge_vec(&self, name: &str, help: &str, label_keys: &[&str]) -> Arc<dyn ApiGaugeVec> {
        let opts = Opts::new(name, help);
        let gv = PromGaugeVec::new(opts, label_keys).expect("valid gauge spec");
        if let Err(e) = self.registry.register(Box::new(gv.clone())) {
            warn!(metric = name, error = %e, "duplicate metric registration; this gauge will not appear in /metrics output");
        }
        Arc::new(GaugeVecAdapter { inner: gv })
    }

    fn histogram_vec(
        &self,
        name: &str,
        help: &str,
        label_keys: &[&str],
    ) -> Arc<dyn ApiHistogramVec> {
        let opts = HistogramOpts::new(name, help);
        let hv = PromHistogramVec::new(opts, label_keys).expect("valid histogram spec");
        if let Err(e) = self.registry.register(Box::new(hv.clone())) {
            warn!(metric = name, error = %e, "duplicate metric registration; this histogram will not appear in /metrics output");
        }
        Arc::new(HistogramVecAdapter { inner: hv })
    }

    fn render(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder
            .encode(&metric_families, &mut buf)
            .expect("encode metrics");
        String::from_utf8(buf).expect("UTF-8 from text encoder")
    }
}

// --- Adapters ---

struct CounterVecAdapter {
    inner: PromCounterVec,
}
struct GaugeVecAdapter {
    inner: PromGaugeVec,
}
struct HistogramVecAdapter {
    inner: PromHistogramVec,
}

impl ApiCounterVec for CounterVecAdapter {
    fn with_labels(&self, labels: &[&str]) -> Arc<dyn ApiCounter> {
        Arc::new(CounterAdapter {
            inner: self.inner.with_label_values(labels),
        })
    }
}

impl ApiGaugeVec for GaugeVecAdapter {
    fn with_labels(&self, labels: &[&str]) -> Arc<dyn ApiGauge> {
        Arc::new(GaugeAdapter {
            inner: self.inner.with_label_values(labels),
        })
    }
}

impl ApiHistogramVec for HistogramVecAdapter {
    fn with_labels(&self, labels: &[&str]) -> Arc<dyn ApiHistogram> {
        Arc::new(HistogramAdapter {
            inner: self.inner.with_label_values(labels),
        })
    }
}

struct CounterAdapter {
    inner: prometheus::Counter,
}
struct GaugeAdapter {
    inner: prometheus::Gauge,
}
struct HistogramAdapter {
    inner: prometheus::Histogram,
}

impl ApiCounter for CounterAdapter {
    fn inc(&self) {
        self.inner.inc();
    }

    #[allow(clippy::cast_precision_loss)]
    fn inc_by(&self, v: u64) {
        self.inner.inc_by(v as f64);
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn get(&self) -> u64 {
        self.inner.get() as u64
    }
}

impl ApiGauge for GaugeAdapter {
    #[allow(clippy::cast_precision_loss)]
    fn set(&self, v: i64) {
        self.inner.set(v as f64);
    }

    fn inc(&self) {
        self.inner.inc();
    }

    fn dec(&self) {
        self.inner.dec();
    }

    #[allow(clippy::cast_possible_truncation)]
    fn get(&self) -> i64 {
        self.inner.get() as i64
    }
}

impl ApiHistogram for HistogramAdapter {
    fn observe(&self, v: f64) {
        self.inner.observe(v);
    }
}
