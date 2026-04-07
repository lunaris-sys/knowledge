// Library interface for knowledge crate.
// Used by benchmarks and integration tests.

pub mod auth;
pub mod db;
pub mod fuse;
pub mod graph;
pub mod identity;
pub mod lifecycle;
pub mod permission;
pub mod schema;
pub mod token;
pub mod token_cache;
pub mod utils;
pub mod write;

pub mod proto {
    #![allow(dead_code)]
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/lunaris.eventbus.rs"));
}
