use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "feiq-cli",
    version = "2.0.0",
    about = "FeiQ Successor Pure Rust CLI client compatibility layer with classic IPMsg"
)]
pub struct CliArgs {
    /// Bind IP address to listen on (default: 0.0.0.0)
    #[arg(short, long, default_value = "0.0.0.0")]
    pub ip: String,

    /// Starting UDP/TCP port to bind (default: 2425)
    #[arg(short, long, default_value_t = feiq_v2::protocol::IPMSG_PORT)]
    pub port: u16,

    /// Custom path to SQLite persistence database file
    #[arg(short, long, default_value = "feiq-cli.db")]
    pub db: PathBuf,

    /// Custom username to advertise to other nodes (defaults to system USER)
    #[arg(short, long)]
    pub username: Option<String>,

    /// Custom hostname to advertise to other nodes (defaults to system HOSTNAME)
    #[arg(short, long)]
    pub hostname: Option<String>,

    /// Print a snapshot of engine statistics and exit
    #[arg(long, default_value_t = false)]
    pub stats: bool,
}

pub fn parse_cli_args() -> CliArgs {
    CliArgs::parse()
}
