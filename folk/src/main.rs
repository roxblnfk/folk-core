//! Folk reference binary (deprecated — use folk.so PHP extension instead).
//!
//! This binary is kept for backwards compatibility but will be removed.
//! The recommended way to run Folk is as a PHP extension:
//!   php -d extension=folk.so folk-server.php

fn main() {
    eprintln!("The folk binary is deprecated. Use the folk.so PHP extension instead:");
    eprintln!("  php -d extension=folk.so folk-server.php");
    eprintln!();
    eprintln!("See https://github.com/Folk-Project for migration instructions.");
    std::process::exit(1);
}
