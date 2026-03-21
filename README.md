# knowledge

The Lunaris knowledge daemon implements the two-layer write architecture described in the blueprint. It consumes events from the Event Bus, stores them in SQLite, promotes relevant events to Ladybug (an embedded property graph database), and exposes a Cypher query interface over a Unix socket.

## Architecture

```
Event Bus (consumer)
    ↓
Ring buffer (10k slots, three-tier backpressure)
    ↓ batch write every 500ms or 1000 events
SQLite (write store, events table)
    ↓ promotion pass every 30s
Ladybug (query store, graph nodes and relationships)
    ↑
Graph Daemon (Unix socket, read-only Cypher queries)
```

**Why two layers?** SQLite handles high write throughput without blocking. Ladybug handles graph traversal queries that SQLite cannot do efficiently. Events land in SQLite first, then a background pass promotes them to Ladybug as structured graph nodes.

## What gets promoted

In Phase 1A/2A:

- `file.opened` → `File` node, `App` node, `ACCESSED_BY` edge
- `window.focused` → `Session` node, `Event` node

Other event types land in SQLite but are not yet promoted. The promotion pipeline is extensible.

## Query interface

Connect to `LUNARIS_DAEMON_SOCKET` and send `[4-byte big-endian length][UTF-8 Cypher query]`. Receive `[4-byte big-endian length][UTF-8 result]`.

Write queries (CREATE, MERGE, DELETE, SET, REMOVE, DROP) are rejected. The query interface is read-only by design.

Example:
```cypher
MATCH (f:File)-[:ACCESSED_BY]->(a:App) RETURN f.path, a.name LIMIT 10
```

## Running

```bash
LUNARIS_CONSUMER_SOCKET=/run/lunaris/event-bus-consumer.sock \
LUNARIS_DB_PATH=/var/lib/lunaris/knowledge/events.db \
LUNARIS_GRAPH_PATH=/var/lib/lunaris/knowledge/graph \
LUNARIS_DAEMON_SOCKET=/run/lunaris/knowledge.sock \
RUST_LOG=info \
./knowledge
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `LUNARIS_CONSUMER_SOCKET` | `/run/lunaris/event-bus-consumer.sock` | Event Bus consumer socket |
| `LUNARIS_DB_PATH` | `/var/lib/lunaris/knowledge/events.db` | SQLite database path |
| `LUNARIS_GRAPH_PATH` | `/var/lib/lunaris/knowledge/graph` | Ladybug database directory |
| `LUNARIS_DAEMON_SOCKET` | `/run/lunaris/knowledge.sock` | Query interface socket |

## Testing

```bash
cargo test
cargo test --test event_pipeline  # integration test, requires event-bus binary
cargo bench --bench graph_scale   # performance benchmarks
```

## Part of

[Lunaris](https://github.com/lunaris-sys) — a Linux desktop OS built around a system-wide knowledge graph.
