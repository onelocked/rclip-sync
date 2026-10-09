//! `rclip` — share the plain-text clipboard between machines on a local
//! network.

use std::io::{self, Read};
use std::net::Ipv4Addr;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::runtime::Runtime;

mod clipboard;
mod proto;
mod serve;

#[derive(Debug, Parser)]
#[command(
    name = "rclip",
    version,
    about = "Share the plain-text clipboard between machines on a local network",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Port to listen on, and the port assumed for peers that omit one.
    #[arg(long, global = true, default_value_t = proto::DEFAULT_PORT)]
    port: u16,

    /// Address to listen on. Bind to a WireGuard address to keep peers off
    /// the physical network.
    #[arg(long, global = true, default_value = "0.0.0.0")]
    bind: Ipv4Addr,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Mirror local clipboard changes to the given peers.
    Serve {
        /// Peer to sync with, as HOST or HOST:PORT. Repeatable. With none
        /// given, peers are read from the config directory.
        #[arg(long = "peer", value_name = "HOST[:PORT]")]
        peer: Vec<String>,
    },

    /// Print the local clipboard.
    LocalPaste,

    /// Put text on the local clipboard.
    LocalCopy {
        /// Text to copy. Omit, or pass -, to read standard input.
        text: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::LocalPaste => {
            print!("{}", clipboard::get_text()?);
            Ok(())
        }
        Command::LocalCopy { text } => {
            clipboard::set_text(&text_argument(text)?)?;

            // On Wayland the clipboard is served from a thread in this process,
            // so exiting now would leave behind a selection nothing can paste.
            // Stay alive until it is replaced (AGENTS.md §4.4).
            clipboard::wait_for_replacement()?;
            eprintln!("rclip: clipboard replaced, exiting");
            Ok(())
        }
        Command::Serve { peer } => runtime()?.block_on(serve::run(&peer, cli.port, cli.bind)),
    }
}

/// Resolve a `text` argument: the literal string, or standard input when it is
/// absent or `-`.
fn text_argument(text: Option<String>) -> Result<String> {
    let Some(text) = text else {
        return read_stdin();
    };

    if text == "-" {
        return read_stdin();
    }

    Ok(text)
}

fn read_stdin() -> Result<String> {
    let mut text = String::new();
    io::stdin()
        .read_to_string(&mut text)
        .context("cannot read standard input")?;

    // Piped input almost always ends in a newline the clipboard has no use for.
    Ok(text.trim_end_matches(['\r', '\n']).to_owned())
}

fn runtime() -> Result<Runtime> {
    Runtime::new().context("cannot start the async runtime")
}
