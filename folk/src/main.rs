//! Folk reference binary.
//!
//! Provides a CLI entry point for the Folk application server.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "folk", version = folk_api::FOLK_API_VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Serve {
        #[arg(short, long, default_value = "folk.toml")]
        config: String,
    },
    Version,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Serve { config } => {
            let cfg = folk_core::config::FolkConfig::load_from(&config)?;
            let runtime = folk_runtime_pipe::PipeRuntime::new(folk_runtime_pipe::PipeConfig {
                php: cfg.workers.php.clone(),
                script: cfg.workers.script.clone(),
            });
            let server = folk_core::server::FolkServer::new(cfg, std::sync::Arc::new(runtime));
            server.run().await
        },
        Commands::Version => {
            println!("folk {}", folk_api::FOLK_API_VERSION);
            Ok(())
        },
    }
}
