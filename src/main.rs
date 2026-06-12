use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "cm", about = "Decentralized Solana snapshot cluster manager")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the full daemon (indexer + peer manager + server + metrics + auto-fetch + S3)
    Daemon {
        #[arg(short, long, default_value = "/etc/cm/config.toml")]
        config: PathBuf,
    },
    /// One-shot: fetch best snapshot from peers, then exit
    Fetch {
        #[arg(short, long, default_value = "/etc/cm/config.toml")]
        config: PathBuf,
        /// Bypass the min_slots_behind freshness gate
        #[arg(long)]
        force: bool,
    },
    /// Query running daemon's API, print peer table + snapshot info
    Status {
        #[arg(short, long, default_value = "http://localhost:13080")]
        url: String,
    },
    /// List known peers with latency/bandwidth stats
    Peers {
        #[arg(short, long, default_value = "http://localhost:13080")]
        url: String,
    },
    /// One-shot: upload current best snapshot to S3
    Upload {
        #[arg(short, long, default_value = "/etc/cm/config.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cm=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon { config: path } => {
            let cfg = cm::config::Config::load(&path)?;
            cm::daemon::run(cfg).await
        }
        Commands::Fetch { config: path, force } => {
            let cfg = cm::config::Config::load(&path)?;
            cm::daemon::fetch_once(cfg, force).await
        }
        Commands::Status { url } => {
            cm::daemon::print_status(&url).await
        }
        Commands::Peers { url } => {
            cm::daemon::print_peers(&url).await
        }
        Commands::Upload { config: path } => {
            let cfg = cm::config::Config::load(&path)?;
            cm::daemon::upload_once(cfg).await
        }
    }
}
