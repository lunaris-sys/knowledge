/// Event Bus subscription for the Graph Daemon.
///
/// Subscribes to `permission.*`, `ai.*`, and `schema.*` events to
/// invalidate token caches and reload schemas in real time.

use crate::proto::Event;
use anyhow::Result;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{debug, warn};

const DEFAULT_EVENT_BUS: &str = "/run/lunaris/event-bus-consumer.sock";

/// Parsed events relevant to the Graph Daemon.
#[derive(Debug, Clone)]
pub enum GraphEvent {
    /// An app's permission profile changed; invalidate its token.
    PermissionChanged { app_id: String },
    /// The AI access level changed; invalidate the ai-daemon token.
    AiLevelChanged,
    /// A new entity schema was registered.
    SchemaRegistered { app_id: String },
    /// An entity schema was removed.
    SchemaRemoved { app_id: String },
}

/// Connect to the Event Bus and register as a consumer.
///
/// Registration format (3 newline-terminated lines):
///   Line 1: consumer ID
///   Line 2: comma-separated event patterns
///   Line 3: UID filter
pub async fn connect(consumer_id: &str, uid: u32) -> Result<UnixStream> {
    let socket_path = std::env::var("LUNARIS_CONSUMER_SOCKET")
        .unwrap_or_else(|_| DEFAULT_EVENT_BUS.to_string());

    let mut stream = UnixStream::connect(&socket_path).await?;

    let registration = format!(
        "{consumer_id}\npermission.*,ai.*,schema.*\n{uid}\n"
    );
    stream.write_all(registration.as_bytes()).await?;

    debug!(consumer_id, uid, "registered with event bus");
    Ok(stream)
}

/// Read the next event from an Event Bus consumer stream.
///
/// Returns `None` on clean disconnect or unrecoverable read error.
pub async fn recv_event(stream: &mut UnixStream) -> Option<GraphEvent> {
    // Read 4-byte length prefix.
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > 1024 * 1024 {
        warn!(len, "invalid event length from bus");
        return None;
    }

    // Read protobuf body.
    let mut buf = vec![0u8; len];
    if stream.read_exact(&mut buf).await.is_err() {
        return None;
    }

    let event = match Event::decode(buf.as_slice()) {
        Ok(e) => e,
        Err(e) => {
            warn!("failed to decode event: {e}");
            return None;
        }
    };

    parse_event(&event)
}

/// Map a raw Event Bus event to a GraphEvent.
fn parse_event(event: &Event) -> Option<GraphEvent> {
    let event_type = event.r#type.as_str();

    if event_type == "permission.changed" {
        // Payload contains app_id; extract from source or payload.
        // For now, extract app_id from the source field or payload.
        let app_id = extract_app_id_from_payload(&event.payload)
            .unwrap_or_else(|| event.source.clone());
        return Some(GraphEvent::PermissionChanged { app_id });
    }

    if event_type.starts_with("ai.") {
        return Some(GraphEvent::AiLevelChanged);
    }

    if event_type == "schema.registered" {
        let app_id = extract_app_id_from_payload(&event.payload)
            .unwrap_or_else(|| "unknown".to_string());
        return Some(GraphEvent::SchemaRegistered { app_id });
    }

    if event_type == "schema.removed" {
        let app_id = extract_app_id_from_payload(&event.payload)
            .unwrap_or_else(|| "unknown".to_string());
        return Some(GraphEvent::SchemaRemoved { app_id });
    }

    None
}

/// Try to extract an app_id from an event payload.
/// The payload may be a protobuf PermissionChangedPayload or
/// SchemaRegisteredPayload, both of which have app_id as field 1.
fn extract_app_id_from_payload(payload: &[u8]) -> Option<String> {
    // Both PermissionChangedPayload and SchemaRegisteredPayload have
    // app_id as field 1 (string). We can try a lightweight decode:
    // field 1 = tag 0x0A (field 1, wire type 2 = length-delimited).
    if payload.len() < 3 {
        return None;
    }
    if payload[0] != 0x0A {
        return None;
    }
    let str_len = payload[1] as usize;
    if payload.len() < 2 + str_len {
        return None;
    }
    String::from_utf8(payload[2..2 + str_len].to_vec()).ok()
}

/// Build the registration string for testing.
pub fn registration_string(consumer_id: &str, uid: u32) -> String {
    format!("{consumer_id}\npermission.*,ai.*,schema.*\n{uid}\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registration_format() {
        let s = registration_string("graph-daemon-1000", 1000);
        assert_eq!(s, "graph-daemon-1000\npermission.*,ai.*,schema.*\n1000\n");
    }

    #[test]
    fn test_parse_permission_changed() {
        let event = Event {
            id: "test".into(),
            r#type: "permission.changed".into(),
            timestamp: 1,
            source: "settings".into(),
            pid: 1,
            session_id: "s".into(),
            payload: vec![],
            uid: 0,
            project_id: String::new(),
        };
        match parse_event(&event) {
            Some(GraphEvent::PermissionChanged { app_id }) => {
                assert_eq!(app_id, "settings"); // fallback to source
            }
            other => panic!("expected PermissionChanged, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_permission_changed_with_payload() {
        // Protobuf: field 1 (tag 0x0A), length 8, "com.test"
        let payload = b"\x0A\x08com.test".to_vec();
        let event = Event {
            id: "test".into(),
            r#type: "permission.changed".into(),
            timestamp: 1,
            source: "settings".into(),
            pid: 1,
            session_id: "s".into(),
            payload,
            uid: 0,
            project_id: String::new(),
        };
        match parse_event(&event) {
            Some(GraphEvent::PermissionChanged { app_id }) => {
                assert_eq!(app_id, "com.test");
            }
            other => panic!("expected PermissionChanged, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_ai_level_changed() {
        let event = Event {
            id: "test".into(),
            r#type: "ai.level_changed".into(),
            timestamp: 1,
            source: "settings".into(),
            pid: 1,
            session_id: "s".into(),
            payload: vec![],
            uid: 0,
            project_id: String::new(),
        };
        assert!(matches!(parse_event(&event), Some(GraphEvent::AiLevelChanged)));
    }

    #[test]
    fn test_parse_schema_registered() {
        let payload = b"\x0A\x08com.anki".to_vec();
        let event = Event {
            id: "test".into(),
            r#type: "schema.registered".into(),
            timestamp: 1,
            source: "install".into(),
            pid: 1,
            session_id: "s".into(),
            payload,
            uid: 0,
            project_id: String::new(),
        };
        match parse_event(&event) {
            Some(GraphEvent::SchemaRegistered { app_id }) => {
                assert_eq!(app_id, "com.anki");
            }
            other => panic!("expected SchemaRegistered, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_unrelated_event() {
        let event = Event {
            id: "test".into(),
            r#type: "file.opened".into(),
            timestamp: 1,
            source: "ebpf".into(),
            pid: 1,
            session_id: "s".into(),
            payload: vec![],
            uid: 1000,
            project_id: String::new(),
        };
        assert!(parse_event(&event).is_none());
    }

    #[test]
    fn test_extract_app_id_from_payload() {
        let payload = b"\x0A\x05hello".to_vec();
        assert_eq!(extract_app_id_from_payload(&payload), Some("hello".into()));

        assert_eq!(extract_app_id_from_payload(&[]), None);
        assert_eq!(extract_app_id_from_payload(&[0x0A]), None);
        assert_eq!(extract_app_id_from_payload(&[0x12, 0x03, b'a', b'b', b'c']), None); // wrong tag
    }
}
