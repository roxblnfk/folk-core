use std::io::Write;

use folk_core::config::{FolkConfig, RuntimeKind};
use tempfile::NamedTempFile;

#[test]
fn default_config_is_usable() {
    let cfg = FolkConfig::default();
    assert_eq!(cfg.workers.count, 4);
    assert_eq!(cfg.workers.max_jobs, 1000);
    assert_eq!(cfg.server.runtime, RuntimeKind::Pipe);
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
    assert_eq!(cfg.server.runtime, RuntimeKind::Pipe);
}
