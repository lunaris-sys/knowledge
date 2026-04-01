// Library interface for knowledge crate.
// Used by benchmarks and integration tests.

pub mod db;
pub mod fuse;
pub mod graph;
pub mod utils;

pub mod proto {
    #![allow(dead_code)]
    #![allow(clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/lunaris.eventbus.rs"));
}
