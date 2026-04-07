use crate::db;
use crate::proto::Event;
use anyhow::Result;
use prost::Message;
use sqlx::SqlitePool;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time;
use tracing::{debug, error, info, warn};

/// Maximum number of events in the ring buffer before we start dropping.
const RING_BUFFER_CAPACITY: usize = 10_000;

/// Write a batch when this many events have accumulated.
const BATCH_SIZE_THRESHOLD: usize = 1_000;

/// Write a batch after this duration even if `BATCH_SIZE_THRESHOLD` is not reached.
const BATCH_TIMEOUT: Duration = Duration::from_millis(500);

/// Connect to the Event Bus as a consumer and stream events into `SQLite`.
///
/// This function runs forever. It reconnects automatically if the Event Bus
/// restarts, with a short delay between attempts.
pub async fn run(consumer_socket: &str, pool: SqlitePool) -> Result<()> {
    loop {
        match connect_and_consume(consumer_socket, &pool).await {
            Ok(()) => {
                // Clean disconnect; Event Bus shut down intentionally.
                info!("event bus disconnected, waiting to reconnect");
            }
            Err(e) => {
                error!("consumer error: {e}, reconnecting in 2s");
            }
        }
        time::sleep(Duration::from_secs(2)).await;
    }
}

/// Connect to the Event Bus consumer socket, register, and consume events.
async fn connect_and_consume(consumer_socket: &str, pool: &SqlitePool) -> Result<()> {
    let mut stream = UnixStream::connect(consumer_socket).await?;
    info!(socket = consumer_socket, "connected to event bus");

    // Send registration: consumer ID followed by subscribed event types.
    // We subscribe to everything ("*") because the Graph Writer stores all events.
    // The promotion pipeline decides later what is worth keeping in Ladybug.
    stream.write_all(b"graph-writer\n").await?;
    stream.write_all(b"*\n").await?;

    info!("registered as consumer, starting event loop");

    // The ring buffer: a Vec we treat as a circular queue.
    // In practice we drain it on every batch write, so it acts more like
    // a bounded staging area than a true circular buffer.
    let mut buffer: Vec<Event> = Vec::with_capacity(RING_BUFFER_CAPACITY);
    let mut interval = time::interval(BATCH_TIMEOUT);

    loop {
        // tokio::select! polls multiple async operations concurrently and
        // executes the branch that completes first. This is how we implement
        // "write when either the buffer is full OR the timeout fires".
        // In C# you would use Task.WhenAny with a CancellationToken.
        tokio::select! {
            // Branch 1: a new event arrived from the socket.
            result = read_event(&mut stream) => {
                match result {
                    Ok(Some(event)) => {
                        admit(&mut buffer, event);
                        if buffer.len() >= BATCH_SIZE_THRESHOLD {
                            flush(&mut buffer, pool).await;
                        }
                    }
                    Ok(None) => {
                        // Clean EOF: event bus closed the connection.
                        debug!("event bus closed connection");
                        flush(&mut buffer, pool).await;
                        return Ok(());
                    }
                    Err(e) => {
                        warn!("read error: {e}");
                        flush(&mut buffer, pool).await;
                        return Err(e);
                    }
                }
            }

            // Branch 2: the 500ms timer fired.
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush(&mut buffer, pool).await;
                }
            }
        }
    }
}

/// Admit an event into the ring buffer, applying the three-tier backpressure
/// policy when the buffer is at capacity.
///
/// Tier 1: check if the incoming event is a duplicate of one already in the buffer.
///         If so, update the existing event's timestamp and discard the new one.
/// Tier 2: if no duplicate, drop the lowest-value event in the buffer.
///         Raw eBPF read/write events with no app-level context are lowest priority.
/// Tier 3: if no low-value event to drop, discard the incoming event.
fn admit(buffer: &mut Vec<Event>, event: Event) {
    if buffer.len() < RING_BUFFER_CAPACITY {
        buffer.push(event);
        return;
    }

    // Tier 1: duplicate detection
    if let Some(existing) = buffer
        .iter_mut()
        .find(|e| e.r#type == event.r#type && e.source == event.source && e.pid == event.pid)
    {
        existing.timestamp = event.timestamp;
        debug!(event_type = %event.r#type, "deduplicated event in buffer");
        return;
    }

    // Tier 2: drop a low-value eBPF read/write event
    if let Some(pos) = buffer.iter().position(|e| {
        e.source == "ebpf"
            && (e.r#type == "file.read" || e.r#type == "file.write")
            && e.payload.is_empty()
    }) {
        buffer.swap_remove(pos);
        buffer.push(event);
        debug!("dropped low-value eBPF event to make room");
        return;
    }

    // Tier 3: drop the incoming event
    warn!(
        event_type = %event.r#type,
        "ring buffer full, dropping incoming event"
    );
}

/// Write all buffered events to `SQLite` and clear the buffer.
async fn flush(buffer: &mut Vec<Event>, pool: &SqlitePool) {
    if buffer.is_empty() {
        return;
    }
    match db::write_batch(pool, buffer).await {
        Ok(n) => debug!(count = n, "flushed batch to SQLite"),
        Err(e) => error!("batch write failed: {e}"),
    }
    buffer.clear();
}

/// Read one length-prefixed protobuf Event from the stream.
/// Returns Ok(None) on clean EOF, Ok(Some(event)) on success, Err on error.
async fn read_event(stream: &mut UnixStream) -> Result<Option<Event>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > 1024 * 1024 {
        anyhow::bail!("invalid event length: {len}");
    }

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let event = Event::decode(buf.as_slice())?;
    Ok(Some(event))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(event_type: &str, source: &str) -> Event {
        Event {
            id: uuid::Uuid::now_v7().to_string(),
            r#type: event_type.to_string(),
            timestamp: 1_000_000,
            source: source.to_string(),
            pid: 1,
            session_id: "session-test".to_string(),
            payload: vec![],
            uid: 0,
        }
    }

    #[test]
    fn admit_under_capacity() {
        let mut buffer = Vec::new();
        admit(&mut buffer, make_event("file.opened", "ebpf"));
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn tier1_deduplication() {
        let mut buffer = Vec::with_capacity(RING_BUFFER_CAPACITY);
        // Fill buffer to capacity with low-value events that are NOT duplicates
        // of our test event (different type).
        for _ in 0..RING_BUFFER_CAPACITY {
            let mut e = make_event("file.read", "ebpf");
            e.pid = 9999; // different pid so tier 1 doesn't match
            buffer.push(e);
        }

        // Add one event that matches what we will try to deduplicate
        let mut original = make_event("window.focused", "wayland");
        original.pid = 42;
        original.timestamp = 100;
        buffer[0] = original;

        let mut duplicate = make_event("window.focused", "wayland");
        duplicate.pid = 42;
        duplicate.timestamp = 200;

        admit(&mut buffer, duplicate);

        // Buffer size unchanged
        assert_eq!(buffer.len(), RING_BUFFER_CAPACITY);
        // Timestamp updated on the existing entry
        let updated = buffer.iter().find(|e| e.r#type == "window.focused").unwrap();
        assert_eq!(updated.timestamp, 200);
    }

    #[test]
    fn tier2_drops_low_value_ebpf() {
        let mut buffer = Vec::with_capacity(RING_BUFFER_CAPACITY);
        for _ in 0..RING_BUFFER_CAPACITY {
            buffer.push(make_event("file.read", "ebpf"));
        }
        let high_value = make_event("app.action", "app:com.example");
        admit(&mut buffer, high_value);
        assert_eq!(buffer.len(), RING_BUFFER_CAPACITY);
        assert!(buffer.iter().any(|e| e.r#type == "app.action"));
    }

    #[test]
    fn tier3_drops_incoming_when_no_low_value_available() {
        let mut buffer = Vec::with_capacity(RING_BUFFER_CAPACITY);
        for _ in 0..RING_BUFFER_CAPACITY {
            buffer.push(make_event("app.action", "app:com.example"));
        }
        let incoming = make_event("network.connection", "ebpf");
        let before_len = buffer.len();
        admit(&mut buffer, incoming);
        assert_eq!(buffer.len(), before_len);
        assert!(!buffer.iter().any(|e| e.r#type == "network.connection"));
    }
}
