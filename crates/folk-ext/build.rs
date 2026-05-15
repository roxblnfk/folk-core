use ext_php_rs_build::{ApiVersion, PHPInfo, emit_check_cfg, emit_php_cfg_flags, find_php};

fn main() {
    let php = find_php().expect("Failed to find PHP");
    let info = PHPInfo::get(&php).expect("Failed to get PHP info");
    let version: ApiVersion = info
        .zend_version()
        .expect("Failed to get Zend version")
        .try_into()
        .expect("Unsupported PHP version");
    emit_php_cfg_flags(version);
    emit_check_cfg();

    // Compile folk_zts.c — thin ZTS wrappers.
    // Need PHP include paths from php-config.
    let php_config = find_php_config();
    let includes = get_php_includes(&php_config);

    let mut build = cc::Build::new();
    build.file("src/folk_zts.c");
    for inc in &includes {
        build.include(inc);
    }

    // ZTS flag.
    let is_zts = info.thread_safety().unwrap_or(false);
    if is_zts {
        build.define("ZTS", None);
        build.define("ZEND_ENABLE_STATIC_TSRMLS_CACHE", "1");
        println!("cargo:rustc-cfg=php_zts");
    }

    build.warnings(false);
    build.compile("folk_zts");

    // PHP extensions are loaded by the PHP process at runtime — symbols like
    // zend_malloc, emalloc etc. are provided by the host. Tell the linker
    // to allow undefined symbols.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }

    println!("cargo:rerun-if-changed=src/folk_zts.c");
}

fn find_php_config() -> String {
    // Try php-config first, then php-config8.3 etc.
    for name in ["php-config", "php-config8.3", "php-config8.2"] {
        if let Ok(output) = std::process::Command::new(name).arg("--version").output() {
            if output.status.success() {
                return name.to_string();
            }
        }
    }
    "php-config".to_string()
}

fn get_php_includes(php_config: &str) -> Vec<String> {
    let output = std::process::Command::new(php_config)
        .arg("--includes")
        .output()
        .expect("failed to run php-config --includes");

    let includes_str = String::from_utf8_lossy(&output.stdout);
    includes_str
        .split_whitespace()
        .filter_map(|s| s.strip_prefix("-I"))
        .map(String::from)
        .collect()
}
