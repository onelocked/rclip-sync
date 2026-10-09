//! Wayland clipboard access via `wl-clipboard-rs`.
//!
//! Everything here goes through the compositor's data-control protocol
//! (`ext-data-control-v1`, falling back to `wlr-data-control-unstable-v1`),
//! which is what allows reading the clipboard without owning a surface. A
//! compositor offering neither cannot be supported, and X11 is out of scope
//! entirely — see AGENTS.md §2 and §10.

use std::io::Read;
use std::thread;

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use wl_clipboard_rs::copy::{self, Options, Source};
use wl_clipboard_rs::paste::{self, Seat};
use wl_clipboard_rs::watch::{self as wl_watch, ClipboardEvent, Offer, Watcher};

use super::{WATCH_BUFFER, accept_change};

/// Text MIME types we accept, most preferred first.
///
/// Offer order belongs to the application and the compositor, so we choose by
/// preference rather than by position. UTF-8 is first because that is what we
/// advertise when writing, so copying between our own machines round-trips
/// through a single encoding.
const TEXT_MIME_TYPES: [&str; 5] = [
    "text/plain;charset=utf-8",
    "text/plain",
    "UTF8_STRING",
    "STRING",
    "TEXT",
];

pub(crate) fn get_text() -> Result<String> {
    match paste::get_contents(
        paste::ClipboardType::Regular,
        Seat::Unspecified,
        paste::MimeType::Text,
    ) {
        Ok((mut pipe, _)) => read_to_string(&mut pipe).context("cannot read the clipboard"),
        // Nothing to read is the clipboard's normal empty state.
        Err(paste::Error::ClipboardEmpty | paste::Error::NoMimeType) => Ok(String::new()),
        Err(error) => Err(error).context("cannot read the clipboard"),
    }
}

pub(crate) fn set_text(text: &str) -> Result<()> {
    // `Options` hands the compositor a pipe and serves the bytes from a thread
    // in this process, so the content outlives the call but not the process
    // (AGENTS.md §4.4).
    Options::new()
        .copy(
            Source::Bytes(text.as_bytes().into()),
            copy::MimeType::Text,
        )
        .context("cannot write the clipboard")
}

pub(crate) fn watch() -> Result<mpsc::Receiver<String>> {
    let mut watcher = Watcher::new(wl_watch::ClipboardType::Regular, Seat::Unspecified)
        .context("cannot watch the clipboard: this compositor exposes no data-control protocol")?;

    let (sender, receiver) = mpsc::channel(WATCH_BUFFER);

    thread::spawn(move || {
        // The first event reports the clipboard's current selection rather than
        // a change. Acting on it would push whatever happened to be on the
        // clipboard to every peer each time the daemon started.
        let mut is_first_event = true;

        loop {
            let event = match watcher.next_event() {
                Ok(Some(event)) => event,
                // Cancelled via `CancelHandle`, which nothing here sends.
                Ok(None) => return,
                Err(error) => {
                    eprintln!("rclip: clipboard watcher stopped: {error}");
                    return;
                }
            };

            if is_first_event {
                is_first_event = false;
                continue;
            }

            // Everything between here and the `blocking_send` is synchronous on
            // purpose. The bytes live in the copying application's process; if
            // it exits or changes its offer while we yield, they are gone for
            // good (AGENTS.md §4.2).
            let text = match event {
                ClipboardEvent::Changed { mime_types, mut offer, .. } => {
                    match read_offer(&mime_types, &mut offer) {
                        Ok(text) => text,
                        Err(error) => {
                            eprintln!("rclip: discarding a clipboard change: {error:#}");
                            continue;
                        }
                    }
                }
                ClipboardEvent::Cleared { .. } => String::new(),
            };

            if !accept_change(&text) {
                continue;
            }

            if sender.blocking_send(text).is_err() {
                return; // nothing is listening any more
            }
        }
    });

    Ok(receiver)
}

/// Drain an offer to a `String` while the offering application is still alive.
///
/// Takes `&mut` because `Offer::receive` consumes the offer: one offer, one
/// read.
fn read_offer(mime_types: &[String], offer: &mut Offer<'_>) -> Result<String> {
    let Some(mime_type) = TEXT_MIME_TYPES
        .iter()
        .find(|wanted| mime_types.iter().any(|offered| offered == *wanted))
    else {
        bail!("clipboard holds no text (offered: {mime_types:?})");
    };

    let mut pipe = offer
        .receive(mime_type)
        .with_context(|| format!("the offerer would not release {mime_type}"))?;

    read_to_string(&mut pipe)
}

/// Read a clipboard pipe to end of string.
///
/// Lossy, on purpose: an application offering bytes that are not valid UTF-8
/// has still made a copy, and mangled text beats silently dropping the event
/// and leaving peers showing a stale clipboard.
fn read_to_string(pipe: &mut impl Read) -> Result<String> {
    let mut bytes = Vec::new();
    pipe.read_to_end(&mut bytes).context("cannot read clipboard data")?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}
