//! Folk reference binary.
//!
//! Provides a CLI entry point for the Folk application server.

use std::sync::Arc;

use clap::{Parser, Subcommand};
use folk_core::config::{FolkConfig, RuntimeKind};
use folk_core::runtime::Runtime;

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
            let cfg = FolkConfig::load_from(&config)?;
            let runtime: Arc<dyn Runtime> = match cfg.server.runtime {
                RuntimeKind::Pipe => Arc::new(folk_runtime_pipe::PipeRuntime::new(
                    folk_runtime_pipe::PipeConfig {
                        php: cfg.workers.php.clone(),
                        script: cfg.workers.script.clone(),
                    },
                )),
                RuntimeKind::Fork => {
                    folk_runtime_fork::ForkRuntime::new(folk_runtime_fork::ForkConfig {
                        php: cfg.workers.php.clone(),
                        script: cfg.workers.script.clone(),
                        boot_timeout: cfg.workers.boot_timeout,
                    })
                    .await?
                },
            };
            let server = folk_core::server::FolkServer::new(cfg, runtime);
            server.run().await
        },
        Commands::Version => {
            println!("folk {}", folk_api::FOLK_API_VERSION);
            Ok(())
        },
    }
}
