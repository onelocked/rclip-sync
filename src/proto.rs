//! Wire protocol: newline-delimited JSON over plain TCP.
//!
//! See AGENTS.md §6 for the normative description. One JSON object per line,
//! `\n`-terminated, UTF-8 throughout.
//!
//! Tests live in `proto/tests.rs`, not inline, so this file reads as the
//! protocol description rather than as protocol plus assertions. They are a
//! submodule rather than the crate's `tests/` integration directory because
//! they reach private items, and because integration tests cannot link a
//! binary-only crate at all — that would need a `lib.rs` this project has no
//! reason to grow.

use std::env;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

/// Version carried in the `v` field of every message.
pub const VERSION: u8 = 1;

/// Port used when neither side specifies one.
pub const DEFAULT_PORT: u16 = 24837;

/// Longest single line we will read. Clipboard payloads larger than this are
/// refused rather than buffered, so a broken or hostile peer cannot make the
/// daemon allocate without bound.
pub const MAX_LINE_BYTES: usize = 1024 * 1024;

/// One protocol message.
///
/// The `t` tag names the variant and the `v` field carries [`VERSION`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Message {
    /// Sent immediately after dialling a peer, for logging only. It carries no
    /// identity claim and grants no access.
    Hello { v: u8, host: String },
    /// Clipboard contents, in either direction.
    Clip { v: u8, text: String },
}

impl Message {
    /// Announcement sent when dialling a peer.
    pub fn hello() -> Self {
        Message::Hello { v: VERSION, host: host_name() }
    }

    /// Clipboard contents.
    pub fn clip(text: impl Into<String>) -> Self {
        Message::Clip { v: VERSION, text: text.into() }
    }

    /// The `v` field of this message.
    pub fn version(&self) -> u8 {
        match self {
            Message::Hello { v, .. } | Message::Clip { v, .. } => *v,
        }
    }
}

/// Best-effort host name for the `hello` announcement.
///
/// Read from the environment rather than resolved, because it is only ever
/// logged and a slow reverse lookup at connect time would be pure cost.
fn host_name() -> String {
    env::var("HOSTNAME")
        .or_else(|_| env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_owned())
}

/// Write one message as a single line.
pub async fn write_message<W>(writer: &mut W, message: &Message) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut line = serde_json::to_vec(message)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    line.push(b'\n');

    writer.write_all(&line).await?;
    writer.flush().await
}

/// Read the next message a peer sends.
///
/// Takes a buffered reader. A peer pipelines — an announced connect followed by
/// a push can share one segment — so a single read can contain several messages,
/// and the bytes after the first newline must survive until the next call. An
/// unbuffered reader has nowhere to keep them and drops them, which loses a
/// message with no error anywhere.
///
/// Returns `Ok(None)` once the peer has closed the connection. Lines carrying a
/// tag this build does not know are skipped, so a newer peer can add message
/// types without breaking an older one. Anything else malformed is an error,
/// because continuing to parse a stream we have lost track of is worse than
/// dropping the connection.
pub async fn read_message<R>(reader: &mut BufReader<R>) -> Result<Option<Message>>
where
    R: AsyncRead + Unpin,
{
    loop {
        let Some(line) = read_line(reader).await? else {
            return Ok(None);
        };

        match parse_line(&line)? {
            Parsed::Message(message) => return Ok(Some(message)),
            Parsed::UnknownTag => continue,
        }
    }
}

enum Parsed {
    Message(Message),
    UnknownTag,
}

/// Read up to and including the next newline, dropping the terminator.
///
/// Returns `Ok(None)` only for a clean end of stream. A stream that ends
/// mid-line is a truncated message, which is an error: the peer vanished
/// between writing two halves of a line.
async fn read_line<R>(reader: &mut BufReader<R>) -> Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut line = Vec::new();

    loop {
        // Borrowed rather than copied, and `consume`d after the bytes have been
        // read out. Whatever follows the newline stays buffered for the next
        // call, which is the entire point.
        let available = reader.fill_buf().await?;

        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-message",
                )
                .into())
            };
        }

        // Split at the terminator when there is one, and take the whole buffer
        // when there is not. Either way the bytes go into `line` and out of the
        // buffer, and whatever follows the newline stays buffered for the next
        // call — which is the entire reason this is a `BufReader`.
        let (take, consumed, terminated) = match available.iter().position(|byte| *byte == b'\n') {
            Some(end) => (end, end + 1, true),
            None => (available.len(), available.len(), false),
        };

        line.extend_from_slice(&available[..take]);
        reader.consume(consumed);

        // Checked on the assembled line and before returning it, so the cap
        // does not depend on which buffer happened to carry the terminator: a
        // payload ending exactly at the limit is accepted, and one byte more is
        // refused even if its newline arrives in the same read as the overflow.
        if line.len() > MAX_LINE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("message exceeds the {MAX_LINE_BYTES} byte limit"),
            )
            .into());
        }

        if terminated {
            return Ok(Some(line));
        }
    }
}

fn parse_line(line: &[u8]) -> Result<Parsed> {
    #[derive(Deserialize)]
    struct Tag {
        t: String,
    }

    let tag: Tag = serde_json::from_slice(line)
        .map_err(|error| malformed(format!("not a message object: {error}")))?;

    if !matches!(tag.t.as_str(), "hello" | "clip") {
        return Ok(Parsed::UnknownTag);
    }

    let message: Message =
        serde_json::from_slice(line).map_err(|error| malformed(format!("bad `{}`: {error}", tag.t)))?;

    if message.version() != VERSION {
        return Err(malformed(format!(
            "protocol version {} is not supported (this build speaks {VERSION})",
            message.version()
        )));
    }

    Ok(Parsed::Message(message))
}

fn malformed(reason: String) -> anyhow::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason).into()
}

/// Resolve a peer the user named on the command line or in the peers file.
///
/// Accepts a bare host name (looked up via the system resolver, on
/// `default_port`), a `host:port` pair, or a full socket address.
pub fn resolve_peer(host: &str, default_port: u16) -> Result<SocketAddr> {
    if let Ok(address) = host.parse::<SocketAddr>() {
        return Ok(address);
    }

    (host, default_port)
        .to_socket_addrs()
        .with_context(|| format!("cannot resolve peer '{host}'"))?
        .next()
        .with_context(|| format!("peer '{host}' resolved to no addresses"))
}

#[cfg(test)]
mod tests;

