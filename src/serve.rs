//! The `rclip serve` daemon: mirrors local clipboard changes to peers, and
//! applies changes peers send us.

use std::env;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{MissedTickBehavior, interval};

use crate::clipboard;
use crate::proto::{self, Message};

/// How often to retry peers that are not currently connected.
///
/// This is the only timer in the program. It is network retry logic, not
/// clipboard polling, and it must not be reused for anything clipboard-related
/// (AGENTS.md §4.1).
const RECONNECT_INTERVAL: Duration = Duration::from_secs(5);

/// Run the daemon until the clipboard watcher stops.
pub async fn run(peers: &[String], port: u16, bind: Ipv4Addr) -> Result<()> {
    let addresses = peer_addresses(peers, port)?;

    let listener = TcpListener::bind((bind, port))
        .await
        .with_context(|| format!("cannot listen on {bind}:{port}"))?;

    eprintln!("rclip: listening on {bind}:{port}");

    // Worth saying out loud: a wildcard bind exposes the clipboard to every host
    // that can route to this port, which on a shared network means anything on
    // the physical LAN. Binding to a WireGuard address keeps the tunnel as the
    // only path in, and is the reason the tunnel is worth running at all.
    if bind.is_unspecified() {
        eprintln!(
            "rclip: bound to all interfaces; set --bind to your WireGuard address to \
             keep peers off the local network"
        );
    }

    for address in &addresses {
        eprintln!("rclip: peer {address}");
    }
    if addresses.is_empty() {
        eprintln!("rclip: no peers configured; this machine will receive, but not send");
    }

    let hub = Hub::new(addresses);
    let mut changes = clipboard::watch()?;

    let mut reconnect = interval(RECONNECT_INTERVAL);
    reconnect.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            changed = changes.recv() => {
                let Some(text) = changed else {
                    // Without a watcher the daemon has nothing left to do, and
                    // silently idling would look like a working sync.
                    bail!("the clipboard watcher stopped");
                };

                hub.broadcast(&text).await;
            }
            _ = reconnect.tick() => hub.reconnect().await,
            accepted = listener.accept() => {
                // One task per peer. A peer that misbehaves ends its own task
                // and nothing else; a panic must not take the daemon with it.
                let (stream, address) =
                    accepted.context("cannot accept a connection")?;

                tokio::spawn(async move {
                    if let Err(error) = handle_peer(stream, address).await {
                        eprintln!("rclip: dropped {}: {error:#}", address.ip());
                    }
                });
            }
        }
    }
}

async fn handle_peer(stream: TcpStream, address: SocketAddr) -> Result<()> {
    // Buffered because a peer may pipeline — an announced connect followed by a
    // push can share one segment — and an unbuffered reader loses whatever
    // follows the first newline.
    let mut reader = BufReader::new(stream);

    loop {
        match proto::read_message(&mut reader).await {
            Ok(Some(message)) => match message {
                Message::Hello { host, .. } => {
                    eprintln!("rclip: {host} connected from {}", address.ip());
                }
                // Never re-broadcast what a peer just sent us: echo suppression
                // already stops the loop, and relaying would turn three machines
                // into a token-passing storm (AGENTS.md §5.4).
                Message::Clip { text, .. } => {
                    clipboard::set_text(&text).context("cannot apply a clipboard change")?;
                }
            },
            Ok(None) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
}

/// The set of peers we push local changes to.
struct Hub {
    peers: Mutex<Vec<Peer>>,
}

struct Peer {
    address: SocketAddr,
    /// `None` while disconnected.
    stream: Option<TcpStream>,
}

impl Hub {
    fn new(addresses: Vec<SocketAddr>) -> Self {
        Hub {
            peers: Mutex::new(
                addresses
                    .into_iter()
                    .map(|address| Peer { address, stream: None })
                    .collect(),
            ),
        }
    }

    /// Send the current clipboard text to every connected peer.
    ///
    /// A peer that stops accepting bytes is marked disconnected and picked up
    /// again by [`Hub::reconnect`]. Losing a peer is never fatal.
    async fn broadcast(&self, text: &str) {
        let message = Message::clip(text);
        let mut peers = self.peers.lock().await;

        // Logged before the peers are touched, so a change is still visible
        // when nothing is connected. Without this, a daemon with no peers is
        // indistinguishable from one that has stopped watching.
        //
        // Deliberately the length and not the text: a clipboard is where
        // passwords and tokens end up, and anything capturing this daemon's
        // stderr would otherwise capture those too. The size is enough to
        // tell "arrived and is sane" from "arrived empty".
        eprintln!("rclip: clipboard changed: {} bytes", text.len());

        for peer in peers.iter_mut() {
            let Some(stream) = peer.stream.as_mut() else {
                continue;
            };

            if proto::write_message(stream, &message).await.is_err() {
                eprintln!("rclip: lost the connection to {}", peer.address);
                peer.stream = None;
            }
        }
    }

    /// Try to connect to peers that are currently down.
    async fn reconnect(&self) {
        let mut peers = self.peers.lock().await;

        for peer in peers.iter_mut() {
            if peer.stream.is_some() {
                continue;
            }

            match TcpStream::connect(peer.address).await {
                Ok(mut stream) => {
                    // Say hello, so the peer's log records who arrived.
                    // Without this an inbound connection is indistinguishable
                    // from one that never happened, which makes diagnosing a
                    // silent sync impossible from either side.
                    if proto::write_message(&mut stream, &Message::hello())
                        .await
                        .is_ok()
                    {
                        eprintln!("rclip: connected to {}", peer.address);
                        peer.stream = Some(stream);
                    }
                }
                // Still down. Say nothing; trying again every tick is the point.
                Err(_) => {}
            }
        }
    }
}

/// Resolve the peers to sync with.
///
/// Command-line peers win. With none given we fall back to the peers file, so
/// the systemd unit does not have to spell the peer list out.
///
/// Having no peers at all is legitimate — that machine receives changes from the
/// others but pushes none of its own, so every machine needs the full list for
/// the mesh to be symmetric. A missing config directory is a note, not an error.
fn peer_addresses(cli_peers: &[String], default_port: u16) -> Result<Vec<SocketAddr>> {
    let mut configured: Vec<String> = cli_peers.to_vec();

    if configured.is_empty() {
        match peers_file() {
            Ok(path) if path.exists() => configured = read_peers_file(&path)?,
            Ok(path) => eprintln!("rclip: no peers file at {}", path.display()),
            Err(error) => eprintln!("rclip: no peers configured ({error:#})"),
        }
    }

    configured
        .iter()
        .map(|peer| proto::resolve_peer(peer, default_port))
        .collect()
}

fn read_peers_file(path: &Path) -> Result<Vec<String>> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;

    Ok(contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect())
}

fn peers_file() -> Result<PathBuf> {
    Ok(config_dir()?.join("peers"))
}

/// Where rclip keeps its configuration.
///
/// `RCLIP_CONFIG_DIR` wins, then the platform's per-user config directory. Read
/// from the environment rather than per-`cfg`, so no target-OS check appears
/// outside the clipboard modules (AGENTS.md §5.1).
fn config_dir() -> Result<PathBuf> {
    if let Some(dir) = env::var_os("RCLIP_CONFIG_DIR") {
        return Ok(PathBuf::from(dir));
    }

    let base = env::var_os("XDG_CONFIG_HOME")
        .or_else(|| env::var_os("APPDATA"))
        .context("set XDG_CONFIG_HOME, APPDATA or RCLIP_CONFIG_DIR to locate the config directory")?;

    Ok(PathBuf::from(base).join("rclip-sync"))
}
