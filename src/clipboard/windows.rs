//! Windows clipboard access via `clipboard-win`.
//!
//! Changes arrive as `WM_CLIPBOARDUPDATE` posted to a hidden message-only
//! window, which `clipboard_win::Monitor` owns and waits on. Nothing here polls
//! — see AGENTS.md §4.1 and §10.

use std::thread;

use anyhow::{Context, Result, anyhow};
use clipboard_win::{Clipboard, Monitor, formats, Getter, Setter};
use tokio::sync::mpsc;

use super::{WATCH_BUFFER, accept_change};

/// How many times to retry opening the clipboard.
///
/// Only one process can hold it open at a time, so a momentary failure is
/// routine rather than exceptional.
const OPEN_ATTEMPTS: usize = 10;

pub(crate) fn get_text() -> Result<String> {
    let _clipboard =
        Clipboard::new_attempts(OPEN_ATTEMPTS).context("cannot open the clipboard")?;

    // Copying an image or a file leaves no text format on the clipboard. That
    // is a normal empty state, not a failure worth propagating.
    if !clipboard_win::is_format_avail(formats::CF_UNICODETEXT) {
        return Ok(String::new());
    }

    let mut text = String::new();
    formats::Unicode
        .read_clipboard(&mut text)
        .context("cannot read the clipboard")?;

    Ok(text)
}

pub(crate) fn set_text(text: &str) -> Result<()> {
    let _clipboard =
        Clipboard::new_attempts(OPEN_ATTEMPTS).context("cannot open the clipboard")?;

    formats::Unicode
        .write_clipboard(&text)
        .context("cannot write the clipboard")
}

pub(crate) fn watch() -> Result<mpsc::Receiver<String>> {
    let (sender, receiver) = mpsc::channel(WATCH_BUFFER);

    // `Monitor` is !Send: it is bound to the thread that created it, so it has
    // to be built here rather than moved in from the caller. That also means
    // setup failures can only be reported back over this channel.
    let (ready_sender, ready_receiver) = std::sync::mpsc::channel();

    thread::spawn(move || {
        let mut monitor = match Monitor::new() {
            Ok(monitor) => monitor,
            Err(error) => {
                let _ = ready_sender.send(Err(error.to_string()));
                return;
            }
        };

        let _ = ready_sender.send(Ok(()));

        loop {
            match monitor.recv() {
                Ok(true) => {}
                // Only a `Shutdown` request produces this, and nothing sends one.
                Ok(false) => return,
                Err(error) => {
                    eprintln!("rclip: clipboard watcher stopped: {error}");
                    return;
                }
            }

            let text = match get_text() {
                Ok(text) => text,
                Err(error) => {
                    // The clipboard is global with no owner-liveness guarantee,
                    // so occasionally reporting a change we can no longer read
                    // is expected. Drop the event, keep the watcher alive.
                    eprintln!("rclip: discarding a clipboard change: {error:#}");
                    continue;
                }
            };

            if !accept_change(&text) {
                continue;
            }

            if sender.blocking_send(text).is_err() {
                return; // nothing is listening any more
            }
        }
    });

    match ready_receiver.recv() {
        Ok(Ok(())) => Ok(receiver),
        Ok(Err(error)) => Err(anyhow!("cannot watch the clipboard: {error}")),
        Err(_) => Err(anyhow!("the clipboard watcher thread died before it started")),
    }
}
