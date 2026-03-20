#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]

mod proto {
    #![allow(dead_code)]
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/lunaris.eventbus.rs"));
}

mod daemon;
mod db;
mod graph;
mod promotion;
mod writer;

use anyhow::Result;
use tracing::info;

const DEFAULT_CONSUMER_SOCKET: &str = "/run/lunaris/event-bus-consumer.sock";
const DEFAULT_DB_PATH: &str = "/var/lib/lunaris/knowledge/events.db";
const DEFAULT_GRAPH_PATH: &str = "/var/lib/lunaris/knowledge/graph";
const DEFAULT_DAEMON_SOCKET: &str = "/run/lunaris/knowledge.sock";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("knowledge=debug".parse()?),
        )
        .init();

    info!("starting knowledge daemon");

    let consumer_socket = std::env::var("LUNARIS_CONSUMER_SOCKET")
        .unwrap_or_else(|_| DEFAULT_CONSUMER_SOCKET.to_string());
    let db_path = std::env::var("LUNARIS_DB_PATH")
        .unwrap_or_else(|_| DEFAULT_DB_PATH.to_string());
    let graph_path = std::env::var("LUNARIS_GRAPH_PATH")
        .unwrap_or_else(|_| DEFAULT_GRAPH_PATH.to_string());
    let daemon_socket = std::env::var("LUNARIS_DAEMON_SOCKET")
        .unwrap_or_else(|_| DEFAULT_DAEMON_SOCKET.to_string());

    // Open SQLite write store
    let pool = db::open(&db_path).await?;
    info!(path = db_path, "sqlite write store ready");

    // Spawn the dedicated Ladybug thread
    let graph = graph::spawn(&graph_path)?;
    info!(path = graph_path, "ladybug query store ready");

    // Run all three components concurrently:
    // - writer: consumes events from the Event Bus into SQLite
    // - promotion: moves events from SQLite into Ladybug periodically
    // - daemon: accepts Cypher queries over a Unix socket
    //
    // tokio::try_join! runs all three concurrently and returns when
    // the first one exits (with either Ok or Err).
    tokio::try_join!(
        writer::run(&consumer_socket, pool.clone()),
        promotion::run(pool, graph.clone()),
        daemon::listen(&daemon_socket, graph),
    )?;

    Ok(())
}
