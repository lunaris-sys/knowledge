/// Event Bus producer for project lifecycle events.
///
/// Connects to the producer socket and sends length-prefixed protobuf
/// Event messages when projects are created, updated, or archived.

use anyhow::Result;
use prost::Message;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use crate::proto::Event;

const DEFAULT_PRODUCER_SOCKET: &str = "/run/lunaris/event-bus-producer.sock";

/// Emits project events to the Event Bus.
pub struct ProjectEmitter {
    socket_path: PathBuf,
}

impl ProjectEmitter {
    /// Create a new emitter. Does not connect immediately.
    pub fn new() -> Self {
        let path = std::env::var("LUNARIS_PRODUCER_SOCKET")
            .unwrap_or_else(|_| DEFAULT_PRODUCER_SOCKET.to_string());
        Self {
            socket_path: PathBuf::from(path),
        }
    }

    /// Emit a `project.created` event.
    pub fn emit_created(
        &self,
        project_id: &str,
        name: &str,
        root_path: &str,
        inferred: bool,
        confidence: u8,
    ) {
        let payload = crate::proto::ProjectCreatedPayload {
            project_id: project_id.to_string(),
            name: name.to_string(),
            root_path: root_path.to_string(),
            inferred,
            confidence: confidence as u32,
        };
        self.emit("project.created", payload.encode_to_vec());
    }

    /// Emit a `project.updated` event.
    pub fn emit_updated(&self, project_id: &str, name: &str) {
        let payload = crate::proto::ProjectUpdatedPayload {
            project_id: project_id.to_string(),
            name: name.to_string(),
            ..Default::default()
        };
        self.emit("project.updated", payload.encode_to_vec());
    }

    /// Emit a `project.archived` event.
    pub fn emit_archived(&self, project_id: &str, name: &str, root_path: &str) {
        let payload = crate::proto::ProjectArchivedPayload {
            project_id: project_id.to_string(),
            name: name.to_string(),
            root_path: root_path.to_string(),
        };
        self.emit("project.archived", payload.encode_to_vec());
    }

    /// Send an event to the Event Bus (fire-and-forget).
    fn emit(&self, event_type: &str, payload: Vec<u8>) {
        let event = Event {
            id: uuid::Uuid::now_v7().to_string(),
            r#type: event_type.to_string(),
            timestamp: chrono::Utc::now().timestamp_micros(),
            source: "knowledge".to_string(),
            pid: std::process::id(),
            session_id: String::new(),
            payload,
            uid: unsafe { libc::getuid() },
            project_id: String::new(),
        };

        let encoded = event.encode_to_vec();
        let len = (encoded.len() as u32).to_be_bytes();

        if let Err(e) = self.send_raw(&len, &encoded) {
            tracing::debug!("event bus emit failed for {event_type}: {e}");
        }
    }

    fn send_raw(&self, len: &[u8], body: &[u8]) -> Result<()> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.write_all(len)?;
        stream.write_all(body)?;
        stream.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emitter_creation() {
        let e = ProjectEmitter::new();
        // Should not panic, just creates struct.
        assert!(!e.socket_path.as_os_str().is_empty());
    }

    #[test]
    fn emit_to_missing_socket_does_not_panic() {
        let e = ProjectEmitter {
            socket_path: PathBuf::from("/tmp/nonexistent-socket-for-test"),
        };
        // Fire-and-forget: should log debug, not panic.
        e.emit_created("id", "name", "/path", true, 90);
        e.emit_updated("id", "name");
        e.emit_archived("id", "name", "/path");
    }
}
