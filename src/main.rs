#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]

mod proto {
    #![allow(dead_code)]
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/lunaris.eventbus.rs"));
}

mod db;
mod writer;

use anyhow::Result;
use tracing::info;

const DEFAULT_CONSUMER_SOCKET: &str = "/run/lunaris/event-bus-consumer.sock";
const DEFAULT_DB_PATH: &str = "/var/lib/lunaris/knowledge/events.db";

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

    let pool = db::open(&db_path).await?;
    info!(path = db_path, "database ready");

    writer::run(&consumer_socket, pool).await?;

    Ok(())
}
