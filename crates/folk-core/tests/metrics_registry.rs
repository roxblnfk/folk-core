use folk_api::MetricsRegistry;
use folk_core::metrics_registry::MetricsRegistryImpl;

#[test]
fn registers_and_renders_counter() {
    let reg = MetricsRegistryImpl::new();
    let cv = reg.counter_vec("requests_total", "Number of requests", &["method"]);
    cv.with_labels(&["GET"]).inc();
    cv.with_labels(&["GET"]).inc_by(4);
    cv.with_labels(&["POST"]).inc();

    let output = reg.render();
    assert!(output.contains("requests_total"));
    assert!(output.contains("method=\"GET\""));
    assert!(output.contains("method=\"POST\""));
}

#[test]
fn registers_gauge_and_histogram() {
    let reg = MetricsRegistryImpl::new();
    reg.gauge_vec("connections", "active connections", &[])
        .with_labels(&[])
        .set(7);
    reg.histogram_vec("request_seconds", "request duration", &[])
        .with_labels(&[])
        .observe(0.123);

    let output = reg.render();
    assert!(output.contains("connections 7"));
    assert!(output.contains("request_seconds"));
}
