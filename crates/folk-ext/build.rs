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

    // PHP extensions are loaded by the PHP process at runtime — symbols like
    // zend_malloc, emalloc etc. are provided by the host. Tell the linker
    // to allow undefined symbols.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
