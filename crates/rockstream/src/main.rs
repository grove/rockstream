use clap::Parser;
use tracing_subscriber::EnvFilter;

/// RockStream: Massively-parallel incremental view maintenance on SlateDB.
#[derive(Parser, Debug)]
#[command(name = "rockstream", version, about, long_about = None)]
struct Cli {
    /// Subcommand to execute.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Parser, Debug)]
enum Command {
    /// Start the RockStream server.
    Start {
        /// Storage directory for local data.
        #[arg(long, default_value = "./data")]
        storage: String,

        /// Role to run as (all, worker, control, gateway, frontier).
        #[arg(long, default_value = "all")]
        role: String,
    },
    /// Print version information.
    Version,
}

fn main() {
    // Initialize tracing with env-filter support.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::Start { storage, role }) => {
            tracing::info!(storage = %storage, role = %role, "starting rockstream");
            println!("RockStream starting with storage={storage}, role={role}");
        }
        Some(Command::Version) => {
            println!("rockstream {}", env!("CARGO_PKG_VERSION"));
        }
        None => {
            println!(
                "RockStream v{}. Use --help for usage.",
                env!("CARGO_PKG_VERSION")
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_help() {
        // Verify that parsing --help triggers the help display (clap exits with error on --help).
        let result = Cli::try_parse_from(["rockstream", "--help"]);
        assert!(result.is_err()); // clap exits on --help
    }

    #[test]
    fn cli_parses_start() {
        let cli = Cli::try_parse_from(["rockstream", "start", "--storage", "/tmp/data"]).unwrap();
        match cli.command {
            Some(Command::Start { storage, .. }) => assert_eq!(storage, "/tmp/data"),
            _ => panic!("expected Start command"),
        }
    }

    #[test]
    fn cli_parses_version_subcommand() {
        let cli = Cli::try_parse_from(["rockstream", "version"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Version)));
    }

    #[test]
    fn cli_no_args_succeeds() {
        let cli = Cli::try_parse_from(["rockstream"]).unwrap();
        assert!(cli.command.is_none());
    }
}
