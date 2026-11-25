use clap::{Args, Parser, Subcommand};
use lattice_miner::*;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

/// Mine Lattice through a node: templates in, nonces out.
#[derive(Parser)]
#[command(name = "lattice-miner", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Mine in rounds until stopped (SIGINT or SIGTERM). The settings file
    /// (worker, workers, batchSize, recipients, minWork,
    /// minBlockIntervalSeconds, templateTimeoutSeconds,
    /// roundDeadlineMultiplier) is read again before every round.
    Run(RunArgs),
    /// Check a settings file and exit: 0 if `run` would accept it, 2 with
    /// the reason if not.
    Check {
        #[arg(long, default_value = "miner.json")]
        config: PathBuf,
    },
}

#[derive(Args)]
#[command(group = clap::ArgGroup::new("auth").required(true))]
struct RunArgs {
    /// The node's URL: its loopback API (with --cookie-file) or its remote
    /// operator listener (with --token-file).
    #[arg(long)]
    node: String,
    /// A bearer token file. The token needs POST /mining/templates,
    /// POST /mining/work and GET /status, and GET /mining/events to be pushed
    /// template changes instead of polling.
    #[arg(long, group = "auth")]
    token_file: Option<PathBuf>,
    /// The node's loopback RPC cookie file.
    #[arg(long, group = "auth")]
    cookie_file: Option<PathBuf>,
    /// The miner's settings.
    #[arg(long, default_value = "miner.json")]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = match Cli::parse().command {
        Command::Run(args) => args,
        Command::Check { config } => match MinerConfig::load(&config) {
            Ok(_) => {
                println!("{}: ok", config.display());
                return;
            }
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(2);
            }
        },
    };
    // The first SIGINT or SIGTERM stops mining once every hit found is
    // submitted; a second stops it at once.
    let (cancel, force) = (CancellationToken::new(), CancellationToken::new());
    let (stop, now) = (cancel.clone(), force.clone());
    tokio::spawn(async move {
        #[cfg(unix)]
        let mut term = {
            use tokio::signal::unix::{SignalKind, signal};
            signal(SignalKind::terminate()).expect("SIGTERM handler")
        };
        for token in [stop, now] {
            #[cfg(unix)]
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            token.cancel();
        }
    });
    let auth = match (args.token_file, args.cookie_file) {
        (Some(token), _) => Auth::Bearer(token),
        (None, Some(cookie)) => Auth::Cookie(cookie),
        (None, None) => unreachable!("clap requires one"),
    };
    let options = RunOptions {
        node: args.node,
        auth,
        config: args.config,
    };
    if let Err(e) = run(options, cancel, force).await {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
}
