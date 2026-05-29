use clap::{Parser, ValueEnum};
use std::path::Path;
use tracing_subscriber::EnvFilter;

use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_runtime::explain::render_explain;

/// RockStream: Massively-parallel incremental view maintenance on SlateDB.
#[derive(Parser, Debug)]
#[command(name = "rockstream", version, about, long_about = None)]
struct Cli {
    /// Subcommand to execute.
    #[command(subcommand)]
    command: Option<Command>,
}

/// Valid roles for a RockStream node.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Role {
    /// Run all roles in a single process (embedded profile).
    All,
    /// Run as a worker node only.
    Worker,
    /// Run as a control-plane node only.
    Control,
    /// Run as a gateway node only.
    Gateway,
    /// Run as a frontier coordinator only.
    Frontier,
}

#[derive(Parser, Debug)]
enum Command {
    /// Start the RockStream server.
    Start {
        /// Storage directory for local data.
        #[arg(long, default_value = "./data")]
        storage: String,

        /// Role to run as.
        #[arg(long, default_value = "all", value_enum)]
        role: Role,
    },
    /// Print the operator graph with merge-law annotations for a view.
    ///
    /// Equivalent to `EXPLAIN INCREMENTAL <view>` (DESIGN.md §14.8).
    /// Prints the merge law (`WeightAdd/v1`, `MaxRegister/v1`, etc.) or the
    /// `not_merge_safe_reason` for every operator in the plan.
    Explain {
        /// View name to explain (e.g. `sales_by_product`).
        view: String,
    },
    /// Print version information.
    Version,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Initialize tracing with env-filter support.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::Start { storage, role }) => {
            tracing::info!(storage = %storage, role = ?role, "starting rockstream");

            let storage_path = Path::new(&storage);
            if let Err(e) = std::fs::create_dir_all(storage_path) {
                tracing::error!(
                    error = %e,
                    path = %storage_path.display(),
                    "RS-0003: failed to create storage directory"
                );
                std::process::exit(1);
            }

            // Audit: server started
            let audit_path = storage_path.join("audit.jsonl");
            let audit_log = match rockstream_control::audit::FileAuditLog::open(&audit_path) {
                Ok(log) => log,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        path = %audit_path.display(),
                        "RS-0003: failed to open audit log"
                    );
                    std::process::exit(1);
                }
            };
            let event =
                rockstream_types::audit::AuditEvent::now("system", "server.started", "rockstream")
                    .with_detail(format!("storage={storage}, role={role:?}"));
            audit_log
                .append(&event)
                .expect("failed to write audit event");

            // Run no-op pipeline
            let result = rockstream_runtime::pipeline::run_noop_pipeline(storage_path).await;
            tracing::info!(epochs = result.epochs_completed, "pipeline completed");

            // Create support bundle
            let bundle_path =
                rockstream_runtime::support_bundle::create_support_bundle(storage_path)
                    .expect("failed to create support bundle");
            tracing::info!(path = %bundle_path.display(), "support bundle written");

            // Audit: server stopped
            let event =
                rockstream_types::audit::AuditEvent::now("system", "server.stopped", "rockstream");
            audit_log
                .append(&event)
                .expect("failed to write audit event");

            println!("RockStream completed: {result:?}");
        }
        Some(Command::Explain { view }) => {
            // Build a representative demo plan that covers the merge-law
            // annotations for SUM/COUNT/AVG/MIN/MAX.
            // In a future version this will look up the view from the catalog.
            let plan = PlanNode::Aggregate {
                input: Box::new(PlanNode::Source { name: view.clone() }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![AggregateExpr {
                    func: AggregateFunc::Sum,
                    input: Expr::Column(1),
                    distinct: false,
                }],
            };
            let output = render_explain(&view, &plan);
            print!("{output}");
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

    #[test]
    fn cli_parses_explain_subcommand() {
        let cli = Cli::try_parse_from(["rockstream", "explain", "sales_by_product"]).unwrap();
        match cli.command {
            Some(Command::Explain { view }) => assert_eq!(view, "sales_by_product"),
            _ => panic!("expected Explain command"),
        }
    }
}
