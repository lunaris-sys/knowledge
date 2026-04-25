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
mod project;
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

use anyhow::{bail, Result};
use tracing::{info, warn};

const DEFAULT_CONSUMER_SOCKET: &str = "/run/lunaris/event-bus-consumer.sock";
const DEFAULT_DB_PATH: &str = "/var/lib/lunaris/knowledge/events.db";
const DEFAULT_GRAPH_PATH: &str = "/var/lib/lunaris/knowledge/graph";
const DEFAULT_DAEMON_SOCKET: &str = "/run/lunaris/knowledge.sock";
const DEFAULT_TIMELINE_MOUNT: &str = ".timeline";

/// Pick the daemon socket path with a graceful fallback for non-root
/// runs. The hardcoded `/run/lunaris/` default requires write access
/// we don't have outside privileged launchers; if nothing is pinned
/// via `LUNARIS_DAEMON_SOCKET` and XDG_RUNTIME_DIR is available, use
/// that. The daemon itself thus starts cleanly in a normal dev
/// session even if the launcher script forgets to set the env var.
fn pick_daemon_socket() -> String {
    if let Ok(p) = std::env::var("LUNARIS_DAEMON_SOCKET") {
        return p;
    }
    if let Ok(xdg) = std::env::var("XDG_RUNTIME_DIR") {
        let path = format!("{xdg}/lunaris/knowledge.sock");
        if let Some(parent) = std::path::Path::new(&path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        return path;
    }
    DEFAULT_DAEMON_SOCKET.to_string()
}

/// Check whether `path` is currently a mount point. Reads
/// `/proc/self/mountinfo` directly so we don't depend on the
/// `mountpoint(1)` binary being installed. Returns `false` on any
/// error — the caller then tries to mount normally (which will fail
/// with a clear error if it actually IS a mount).
fn is_mountpoint(path: &str) -> bool {
    let Ok(content) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    // mountinfo layout per proc(5):
    //   id parent major:minor root mount-point mount-options ... - fstype source super-opts
    // Index 4 (0-based) is the mount point. Space-separated tokens;
    // paths with spaces are octal-escaped but we match literal so
    // that's fine for our `~/.timeline`.
    for line in content.lines() {
        if let Some(target) = line.split_whitespace().nth(4) {
            if target == path {
                return true;
            }
        }
    }
    false
}

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
    let daemon_socket = pick_daemon_socket();
    let timeline_mount = std::env::var("LUNARIS_TIMELINE_MOUNT").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        format!("{home}/{DEFAULT_TIMELINE_MOUNT}")
    });
    info!(%daemon_socket, "daemon socket path resolved");

    // Open SQLite write store
    let pool = db::open(&db_path).await?;
    info!(path = db_path, "sqlite write store ready");

    // Spawn the dedicated Ladybug thread
    let graph = graph::spawn(&graph_path)?;
    info!(path = graph_path, "ladybug query store ready");

    // FUSE runs on a dedicated OS thread (blocking mount).
    //
    // Before attempting to mount, check if `timeline_mount` is already
    // a (possibly stale) mount point. If a previous daemon was
    // SIGKILL'd without its FUSE exit handler firing, the kernel
    // keeps the mount registered while the userspace process is gone
    // — calling `fuse::mount` on that path then returns `File exists
    // (os error 17)`. Skip the mount-attempt entirely in that case
    // and point the operator at the launcher script which handles
    // cleanup.
    let fuse_graph = graph.clone();
    let fuse_mount_path = timeline_mount.clone();
    std::thread::Builder::new()
        .name("fuse-timeline".into())
        .spawn(move || {
            if is_mountpoint(&fuse_mount_path) {
                warn!(
                    path = %fuse_mount_path,
                    "FUSE: path already mounted — skipping remount. \
                     Stale mount from a previous run? Fix with \
                     `fusermount -u {fuse_mount_path}` or use \
                     `distro/start-dev.sh` which handles this automatically",
                );
                return;
            }
            if let Err(e) = fuse::mount(&fuse_mount_path, fuse_graph) {
                tracing::error!("FUSE mount failed: {e}");
            }
        })?;

    // Validate-on-startup pass: any project whose root_path vanished
    // since the last run gets pruned (inferred) or archived (explicit).
    // Per docs/architecture/project-system.md §Validation on Access we
    // do not poll periodically; daemon startup is one of the access
    // points the spec calls out. Failures on individual projects do
    // not abort the sweep — they are logged and counted.
    {
        let store = project::ProjectStore::new(graph.clone());
        match store.prune_dead_projects().await {
            Ok(stats) => info!(
                alive = stats.alive,
                pruned = stats.pruned,
                archived = stats.archived,
                errors = stats.errors,
                "startup project validation complete"
            ),
            Err(e) => warn!(
                error = %e,
                "startup project validation failed; continuing without prune"
            ),
        }
    }

    // Project watcher: scans configured directories and watches for changes.
    let project_graph = graph.clone();
    tokio::spawn(async move {
        if let Err(e) = project::watcher::run(project_graph).await {
            tracing::error!("project watcher error: {e}");
        }
    });

    // Run all four components concurrently. `tokio::select!` — not
    // `try_join!` — so a failing task is attributed by name instead
    // of leaving the operator with an anonymous "Error: Permission
    // denied (os error 13)" and no way to tell which task emitted it.
    tokio::select! {
        r = writer::run(&consumer_socket, pool.clone()) => match r {
            Ok(()) => bail!("writer task exited unexpectedly"),
            Err(e) => bail!("writer ({consumer_socket}): {e}"),
        },
        r = promotion::run(pool.clone(), graph.clone()) => match r {
            Ok(()) => bail!("promotion task exited unexpectedly"),
            Err(e) => bail!("promotion: {e}"),
        },
        r = retention::run(pool, graph.clone()) => match r {
            Ok(()) => bail!("retention task exited unexpectedly"),
            Err(e) => bail!("retention: {e}"),
        },
        r = daemon::listen(&daemon_socket, graph) => match r {
            Ok(()) => bail!("daemon listener exited unexpectedly"),
            Err(e) => bail!("daemon listen ({daemon_socket}): {e}"),
        },
    }
}
