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

const CONSUMER_SOCKET: &str = "/run/lunaris/event-bus-consumer.sock";
const DB_PATH: &str = "/var/lib/lunaris/knowledge/events.db";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("knowledge=debug".parse()?),
        )
        .init();

    info!("starting knowledge daemon");

    let pool = db::open(DB_PATH).await?;
    info!(path = DB_PATH, "database ready");

    writer::run(CONSUMER_SOCKET, pool).await?;

    Ok(())
}
