use folk_core::config::{LogConfig, LogFormat};
use folk_core::logging::init;

#[test]
fn init_succeeds_once() {
    let cfg = LogConfig {
        filter: "info".into(),
        format: LogFormat::Text,
    };
    // Don't assert success vs failure: in a test process, another test may
    // have initialized first. Just ensure it doesn't panic.
    let _ = init(&cfg);
}
