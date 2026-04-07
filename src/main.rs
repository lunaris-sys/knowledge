#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]

mod proto {
    #![allow(dead_code)]
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/lunaris.eventbus.rs"));
}

mod auth;
mod backup;
mod daemon;
mod db;
mod events;
mod fuse;
mod graph;
mod identity;
mod lifecycle;
mod migration;
mod permission;
mod promotion;
mod quota;
mod retention;
mod schema;
mod shared;
mod token;
mod token_cache;
mod utils;
mod write;
mod writer;

use anyhow::Result;
use tracing::info;

const DEFAULT_CONSUMER_SOCKET: &str = "/run/lunaris/event-bus-consumer.sock";
const DEFAULT_DB_PATH: &str = "/var/lib/lunaris/knowledge/events.db";
const DEFAULT_GRAPH_PATH: &str = "/var/lib/lunaris/knowledge/graph";
const DEFAULT_DAEMON_SOCKET: &str = "/run/lunaris/knowledge.sock";
const DEFAULT_TIMELINE_MOUNT: &str = ".timeline";

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
    let timeline_mount = std::env::var("LUNARIS_TIMELINE_MOUNT").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        format!("{home}/{DEFAULT_TIMELINE_MOUNT}")
    });

    // Open SQLite write store
    let pool = db::open(&db_path).await?;
    info!(path = db_path, "sqlite write store ready");

    // Spawn the dedicated Ladybug thread
    let graph = graph::spawn(&graph_path)?;
    info!(path = graph_path, "ladybug query store ready");

    // FUSE runs on a dedicated OS thread (blocking mount).
    let fuse_graph = graph.clone();
    std::thread::Builder::new()
        .name("fuse-timeline".into())
        .spawn(move || {
            if let Err(e) = fuse::mount(&timeline_mount, fuse_graph) {
                tracing::error!("FUSE mount failed: {e}");
            }
        })?;

    // Run all four components concurrently:
    // - writer: consumes events from the Event Bus into SQLite
    // - promotion: moves events from SQLite into Ladybug periodically
    // - retention: purges old events and compacts old graph nodes daily
    // - daemon: accepts Cypher queries over a Unix socket
    //
    // tokio::try_join! runs all four concurrently and returns when
    // the first one exits (with either Ok or Err).
    tokio::try_join!(
        writer::run(&consumer_socket, pool.clone()),
        promotion::run(pool.clone(), graph.clone()),
        retention::run(pool, graph.clone()),
        daemon::listen(&daemon_socket, graph),
    )?;

    Ok(())
}
