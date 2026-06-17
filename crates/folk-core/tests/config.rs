use std::collections::HashMap;
use std::io::Write;

use folk_core::config::{FolkConfig, LogConfig, LogFormat};
use tempfile::NamedTempFile;

#[test]
fn default_config_is_usable() {
    let cfg = FolkConfig::default();
    assert_eq!(cfg.workers.count, 4);
    assert_eq!(cfg.workers.max_jobs, 1000);
    assert!(cfg.workers.warmup);
    assert_eq!(cfg.workers.max_concurrent_per_worker, 1);
}

#[test]
fn max_concurrent_per_worker_loads_from_toml() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(
        f,
        r"
        [workers]
        max_concurrent_per_worker = 4
        "
    )
    .unwrap();

    let cfg = FolkConfig::load_from(f.path()).unwrap();
    assert_eq!(cfg.workers.max_concurrent_per_worker, 4);
}

#[test]
fn normalize_clamps_max_concurrent_per_worker() {
    // > 1 is not yet supported: clamp to 1.
    let mut w = FolkConfig::default().workers;
    w.max_concurrent_per_worker = 8;
    w.normalize();
    assert_eq!(w.max_concurrent_per_worker, 1);

    // 0 is invalid: clamp to 1.
    w.max_concurrent_per_worker = 0;
    w.normalize();
    assert_eq!(w.max_concurrent_per_worker, 1);

    // 1 stays 1.
    w.max_concurrent_per_worker = 1;
    w.normalize();
    assert_eq!(w.max_concurrent_per_worker, 1);
}

#[test]
fn loads_from_toml_overrides_defaults() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(
        f,
        r"
        [workers]
        count = 16
        max_jobs = 5000
        "
    )
    .unwrap();

    let cfg = FolkConfig::load_from(f.path()).unwrap();
    assert_eq!(cfg.workers.count, 16);
    assert_eq!(cfg.workers.max_jobs, 5000);
    // Other defaults preserved
    assert_eq!(cfg.server.rpc_socket, "/tmp/folk.sock");
}

#[test]
fn effective_filter_without_plugins() {
    let cfg = LogConfig {
        filter: "info".into(),
        format: LogFormat::Text,
        plugins: HashMap::new(),
    };
    assert_eq!(cfg.effective_filter(), "info");
}

#[test]
fn effective_filter_with_plugin_overrides() {
    let mut plugins = HashMap::new();
    plugins.insert("http".into(), "warn".into());
    plugins.insert("core".into(), "debug".into());
    let cfg = LogConfig {
        filter: "info".into(),
        format: LogFormat::Json,
        plugins,
    };
    let filter = cfg.effective_filter();
    assert!(filter.starts_with("info,"));
    assert!(filter.contains("folk_plugin_http=warn"));
    assert!(filter.contains("folk_core=debug"));
}

#[test]
fn warmup_default_true() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(
        f,
        r"
        [workers]
        count = 2
        "
    )
    .unwrap();

    let cfg = FolkConfig::load_from(f.path()).unwrap();
    assert!(cfg.workers.warmup);
}

#[test]
fn warmup_can_be_disabled() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(
        f,
        r"
        [workers]
        count = 2
        warmup = false
        "
    )
    .unwrap();

    let cfg = FolkConfig::load_from(f.path()).unwrap();
    assert!(!cfg.workers.warmup);
}

#[test]
fn dev_watch_defaults_off() {
    let cfg = FolkConfig::default();
    assert!(!cfg.dev.watch);
    assert!(cfg.dev.watch_paths.contains(&"app".to_string()));
    assert_eq!(cfg.dev.watch_extensions, vec!["php".to_string()]);
    assert_eq!(cfg.dev.debounce, std::time::Duration::from_millis(300));
}

#[test]
fn dev_watch_from_toml() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(
        f,
        r#"
        [dev]
        watch = true
        watch_paths = ["src", "lib"]
        watch_extensions = ["php", "phtml"]
        debounce = "500ms"
        "#
    )
    .unwrap();

    let cfg = FolkConfig::load_from(f.path()).unwrap();
    assert!(cfg.dev.watch);
    assert_eq!(cfg.dev.watch_paths, vec!["src", "lib"]);
    assert_eq!(cfg.dev.watch_extensions, vec!["php", "phtml"]);
    assert_eq!(cfg.dev.debounce, std::time::Duration::from_millis(500));
}

#[test]
fn log_plugins_from_toml() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(
        f,
        r#"
        [log]
        filter = "warn"
        format = "json"

        [log.plugins]
        http = "debug"
        process = "info"
        "#
    )
    .unwrap();

    let cfg = FolkConfig::load_from(f.path()).unwrap();
    assert_eq!(cfg.log.filter, "warn");
    assert_eq!(cfg.log.format, LogFormat::Json);
    let filter = cfg.log.effective_filter();
    assert!(filter.contains("folk_plugin_http=debug"));
    assert!(filter.contains("folk_plugin_process=info"));
}
