//! Platform-agnostic clipboard access.
//!
//! Both platforms expose the same three functions and the same echo-suppression
//! behaviour. Everything that differs between `wl-clipboard-rs` and
//! `clipboard-win` is confined to the platform modules — see AGENTS.md §5.

use std::sync::{Mutex, MutexGuard, OnceLock};

use anyhow::Result;
use tokio::sync::mpsc;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform;

#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
use windows as platform;

/// Depth of the watcher channel.
///
/// Clipboard traffic is bursty but shallow; this only has to absorb copies made
/// faster than the daemon broadcasts them.
const WATCH_BUFFER: usize = 64;

/// Text this process most recently wrote to the clipboard, used to recognise
/// the change event the platform echoes back at us (AGENTS.md §4.3).
static LAST_WRITTEN: OnceLock<Mutex<Option<String>>> = OnceLock::new();

/// The last-written slot.
///
/// A poisoned mutex only means some other thread panicked while holding it.
/// The slot is a cache of the last string we wrote, so recovering the value is
/// always safe and preferable to losing echo suppression entirely.
fn last_written() -> MutexGuard<'static, Option<String>> {
    LAST_WRITTEN
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Blocking read of the current clipboard text.
///
/// An empty or cleared clipboard reads as an empty string; that is a normal
/// state, not a failure.
pub fn get_text() -> Result<String> {
    platform::get_text()
}

/// Blocking write to the local clipboard.
///
/// Records `text` as last-written *before* the write, so the change event the
/// write triggers is recognised as our own echo rather than a user copy.
///
/// On Wayland the bytes are served from a thread inside this process, so the
/// caller must stay alive for the content to remain pasteable — see
/// [`wait_for_replacement`] and AGENTS.md §4.4.
pub fn set_text(text: &str) -> Result<()> {
    *last_written() = Some(text.to_owned());
    platform::set_text(text)
}

/// Block until something other than us replaces the clipboard.
///
/// `rclip local-copy` needs this. A one-shot process that writes the clipboard
/// and exits leaves behind a selection nothing can paste from, because the
/// compositor is still holding a pipe to a process that no longer exists.
pub fn wait_for_replacement() -> Result<()> {
    let mut changes = watch()?;

    // Echo suppression drops our own write, so the first item delivered here is
    // by construction a genuine replacement. `None` means the watcher stopped.
    while changes.blocking_recv().is_some() {}

    Ok(())
}

/// Watch for clipboard changes, returning a stream of the new text.
///
/// Every text yielded has already passed echo suppression, so it always comes
/// from a real copy by the user or a peer. On Wayland the offer has been
/// drained to a fully-owned `String` before it is sent (AGENTS.md §4.2).
///
/// The platform watcher is a blocking iterator, so it runs on its own thread
/// and feeds this channel; there is no timer anywhere in this path
/// (AGENTS.md §4.1).
pub fn watch() -> Result<mpsc::Receiver<String>> {
    platform::watch()
}

/// Decide whether a clipboard change is genuine, or the echo of our own write.
///
/// On a match the record is deliberately kept: Windows double-fires
/// `WM_CLIPBOARDUPDATE` for a single copy, and a record cleared on the first
/// match would let the second fire look like a fresh copy and bounce the text
/// straight back to the peer. See AGENTS.md §4.3.
pub(crate) fn accept_change(text: &str) -> bool {
    let mut last_written = last_written();

    match last_written.as_deref() {
        Some(ours) if ours == text => false,
        _ => {
            *last_written = None;
            true
        }
    }
}
