//! Tests for the wire protocol: framing, size limits, and version handling.
//!
//! These live in their own file rather than inline so `proto.rs` stays readable
//! as protocol documentation. They are a submodule rather than the `tests/`
//! integration directory because they reach private items (`parse_line`,
//! `MAX_LINE_BYTES`) and because integration tests cannot link a binary-only
//! crate — that would need a `lib.rs` this project deliberately does not have.

use super::*;

/// Read every message the given bytes yield.
///
/// A plain byte slice stands in for a socket: it implements `AsyncRead`, and
/// it delivers everything in one read, which is exactly the case where an
/// unbuffered reader used to lose the messages after the first.
async fn read_all(bytes: &[u8]) -> Result<Vec<Message>> {
    let mut reader = BufReader::new(bytes);
    let mut messages = Vec::new();

    while let Some(message) = read_message(&mut reader).await? {
        messages.push(message);
    }

    Ok(messages)
}

fn line(message: &Message) -> Vec<u8> {
    let mut line = serde_json::to_vec(message).unwrap();
    line.push(b'\n');
    line
}

#[tokio::test]
async fn reads_messages_that_arrive_in_one_read() {
    // An announced connect followed immediately by a push can share one
    // segment, and a burst of pushes coalesces the same way.
    let mut bytes = line(&Message::hello());
    bytes.extend(line(&Message::clip("first")));
    bytes.extend(line(&Message::clip("second")));

    let messages = read_all(&bytes).await.unwrap();

    assert_eq!(messages.len(), 3);
    assert!(matches!(messages[0], Message::Hello { .. }));
    assert!(matches!(messages[1], Message::Clip { .. }));
    assert!(matches!(messages[2], Message::Clip { .. }));
}

#[tokio::test]
async fn carries_the_clipboard_text_unchanged() {
    let messages = read_all(&line(&Message::clip("秘密テスト 🙂\nnot a newline")))
        .await
        .unwrap();

    let Message::Clip { text, .. } = &messages[0] else {
        panic!("expected a clip message, got {:?}", messages[0]);
    };

    // Newlines inside the payload are escaped by serde_json, so the framing
    // survives them. Non-ASCII is the interesting half: this is a
    // UTF-8-in, UTF-8-out guarantee and a lossy read anywhere would break
    // it silently.
    assert_eq!(text, "秘密テスト 🙂\nnot a newline");
}

#[tokio::test]
async fn an_empty_stream_is_a_clean_close() {
    assert!(read_all(b"").await.unwrap().is_empty());
}

#[tokio::test]
async fn a_stream_ending_mid_message_is_an_error() {
    let truncated = br#"{"t":"clip","v":1,"text":"half"#;

    let error = read_all(truncated).await.unwrap_err();

    assert!(
        error.to_string().contains("mid-message"),
        "unhelpful error: {error:#}"
    );
}

#[tokio::test]
async fn an_unknown_tag_is_skipped_not_fatal() {
    // Forward compatibility: a newer peer may send message types this build
    // has never heard of, and must not be able to break an older daemon.
    let mut bytes = b"{\"t\":\"future\",\"v\":1}\n".to_vec();
    bytes.extend(line(&Message::clip("after an unknown tag")));

    let messages = read_all(&bytes).await.unwrap();

    assert_eq!(messages.len(), 1);
    assert!(matches!(messages[0], Message::Clip { .. }));
}

#[tokio::test]
async fn a_removed_message_type_is_skipped_like_any_unknown() {
    // `get` and the one-shot commands that sent it are gone. An older peer still
    // dialling and asking must be ignored rather than treated as a protocol
    // error, which is what dropping a variant from the tag list would otherwise
    // risk.
    let mut bytes = b"{\"t\":\"get\",\"v\":1}\n".to_vec();
    bytes.extend(line(&Message::clip("unaffected")));

    let messages = read_all(&bytes).await.unwrap();

    assert_eq!(messages.len(), 1);
    assert!(matches!(messages[0], Message::Clip { .. }));
}

#[tokio::test]
async fn an_unknown_version_is_rejected() {
    let bytes = b"{\"t\":\"clip\",\"v\":99,\"text\":\"x\"}\n";

    let error = read_all(bytes).await.unwrap_err();

    assert!(
        error.to_string().contains("version"),
        "unhelpful error: {error:#}"
    );
}

/// Bytes the JSON envelope adds around a payload.
///
/// The newline terminator is excluded, because that is what the size cap is
/// measured against: `read_line` checks the assembled line before handing back
/// the terminator. Derived rather than hardcoded, because a hand-counted
/// constant is one field rename away from being wrong — and a wrong constant
/// makes the size-limit tests assert the opposite of what they claim.
fn envelope() -> usize {
    line(&Message::clip("")).len() - 1
}

#[tokio::test]
async fn a_line_at_the_limit_is_accepted() {
    // A payload landing exactly on the boundary must pass, rather than being
    // refused for one byte of overshoot.
    let text = "a".repeat(MAX_LINE_BYTES - envelope());
    let bytes = line(&Message::clip(text.clone()));

    // One byte over the cap, once the terminator is counted — the cap is on the
    // message, not on the bytes read off the socket.
    assert_eq!(bytes.len(), MAX_LINE_BYTES + 1);

    let messages = read_all(&bytes).await.unwrap();

    let Message::Clip { text: received, .. } = &messages[0] else {
        panic!("expected a clip message");
    };
    assert_eq!(received.len(), text.len());
}

#[tokio::test]
async fn a_line_over_the_limit_is_refused() {
    let text = "a".repeat(MAX_LINE_BYTES - envelope() + 1);

    let error = read_all(&line(&Message::clip(text))).await.unwrap_err();

    assert!(
        error.to_string().contains("byte limit"),
        "unhelpful error: {error:#}"
    );
}

#[tokio::test]
async fn peers_resolve_to_a_socket_address() {
    // A bare host name, a host:port pair and a full address all resolve, and
    // a host with no port picks up the default rather than failing.
    let address = resolve_peer("127.0.0.1", DEFAULT_PORT).unwrap();
    assert_eq!(address.port(), DEFAULT_PORT);

    let address = resolve_peer("127.0.0.1:1234", DEFAULT_PORT).unwrap();
    assert_eq!(address.port(), 1234);
}
