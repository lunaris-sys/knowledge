/// Integration test: event emitted by a producer lands in SQLite via the Event Bus.
///
/// This test starts real event-bus and knowledge daemon processes,
/// sends a synthetic event over the producer socket, waits for the
/// batch timer to fire, and verifies the event exists in SQLite.
///
/// Both binaries must be built before running this test:
///   cargo build --manifest-path ../event-bus/Cargo.toml
///   cargo build --manifest-path ../knowledge/Cargo.toml
///
/// The test uses temporary socket paths and a temporary database to
/// avoid interfering with a running system.
use prost::Message;
use sqlx::sqlite::SqlitePoolOptions;
use std::io::Write;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

// Include generated protobuf types.
// We build the proto in knowledge's build.rs so we can use them here too.
mod proto {
    include!(concat!(env!("OUT_DIR"), "/lunaris.eventbus.rs"));
}

/// Locate a binary in the Cargo target directory.
/// Cargo sets CARGO_MANIFEST_DIR to the knowledge crate root.
/// The event-bus binary is in the sibling repo's target dir.
fn binary_path(name: &str) -> PathBuf {
    // Walk up from knowledge/ to the workspace parent, then into event-bus/target
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = PathBuf::from(&manifest_dir)
        .parent()
        .unwrap()
        .to_path_buf();

    // In the same workspace root the other repos live as siblings.
    // Cargo builds each binary into its own target/debug/ directory.
    workspace_root
        .join(name)
        .join("target")
        .join("debug")
        .join(name)
}

/// Wait until a Unix socket file exists, polling every 50ms.
/// Panics if the timeout is exceeded.
fn wait_for_socket(path: &str, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        if std::path::Path::new(path).exists() {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "timed out waiting for socket: {path}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Send a single Event as a length-prefixed protobuf message over a Unix socket.
fn send_event(socket_path: &str, event: &proto::Event) {
    let encoded = event.encode_to_vec();
    let len = u32::try_from(encoded.len())
        .expect("event too large")
        .to_be_bytes();

    let mut stream = UnixStream::connect(socket_path)
        .unwrap_or_else(|e| panic!("failed to connect to producer socket {socket_path}: {e}"));

    stream.write_all(&len).expect("failed to write length");
    stream.write_all(&encoded).expect("failed to write event");
    stream.shutdown(Shutdown::Both).ok();
}

/// Helper that kills a child process when dropped.
/// This ensures cleanup even if the test panics.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

#[tokio::test]
async fn event_lands_in_sqlite() {
    // Use a temporary directory so tests do not interfere with each other
    // or with a running system.
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let producer_socket = tmp.path().join("producer.sock");
    let consumer_socket = tmp.path().join("consumer.sock");
    let db_path = tmp.path().join("events.db");

    let producer_socket_str = producer_socket.to_str().unwrap();
    let consumer_socket_str = consumer_socket.to_str().unwrap();
    let db_path_str = db_path.to_str().unwrap();

    // Start the event-bus daemon.
    let _event_bus = KillOnDrop(
        Command::new(binary_path("event-bus"))
            .env("LUNARIS_PRODUCER_SOCKET", producer_socket_str)
            .env("LUNARIS_CONSUMER_SOCKET", consumer_socket_str)
            .env("RUST_LOG", "error") // suppress noise in test output
            .spawn()
            .expect("failed to start event-bus"),
    );

    // Wait for the event-bus sockets to appear.
    wait_for_socket(producer_socket_str, Duration::from_secs(5));
    wait_for_socket(consumer_socket_str, Duration::from_secs(5));

    // Start the knowledge daemon.
    let _knowledge = KillOnDrop(
        Command::new(binary_path("knowledge"))
            .env("LUNARIS_CONSUMER_SOCKET", consumer_socket_str)
            .env("LUNARIS_DB_PATH", db_path_str)
            .env("RUST_LOG", "error")
            .spawn()
            .expect("failed to start knowledge"),
    );

    // Give knowledge time to connect and register as a consumer.
    std::thread::sleep(Duration::from_millis(200));

    // Send a synthetic event.
    let event = proto::Event {
        id: "01950000-0000-7000-8000-000000000099".to_string(),
        r#type: "file.opened".to_string(),
        timestamp: 1_000_000,
        source: "test".to_string(),
        pid: 42,
        session_id: "session-integration-test".to_string(),
        payload: vec![],
    };

    send_event(producer_socket_str, &event);

    // Wait for the knowledge daemon's 500ms batch timer to fire and write to SQLite.
    std::thread::sleep(Duration::from_millis(800));

    // Open the SQLite database directly and check the event is there.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite:{db_path_str}"))
        .await
        .expect("failed to open db");

    let row: (String, String) =
        sqlx::query_as("SELECT id, type FROM events WHERE id = ?")
            .bind(&event.id)
            .fetch_one(&pool)
            .await
            .expect("event not found in SQLite");

    assert_eq!(row.0, event.id);
    assert_eq!(row.1, event.r#type);
}
