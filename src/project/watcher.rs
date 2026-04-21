/// Filesystem watcher for project detection.
///
/// Runs an initial scan of configured directories, then watches for
/// filesystem changes (new `.project` files, `git init`, etc.) and
/// creates/updates/archives projects in the graph.

use std::path::Path;
use std::sync::Arc;

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::graph::GraphHandle;
use crate::project::emitter::ProjectEmitter;
use crate::project::signals::{SignalDetector, SignalType};
use crate::project::watch_config::WatchConfig;
use crate::project::{Project, ProjectParser, ProjectStatus, ProjectStore};

/// Project watcher: initial scan + live filesystem monitoring.
pub struct ProjectWatcher {
    config: WatchConfig,
    store: Arc<ProjectStore>,
    emitter: ProjectEmitter,
}

impl ProjectWatcher {
    /// Create a new watcher.
    pub fn new(config: WatchConfig, store: Arc<ProjectStore>) -> Self {
        Self {
            config,
            store,
            emitter: ProjectEmitter::new(),
        }
    }

    /// Scan all configured directories once and register projects.
    ///
    /// Never propagates per-directory errors up. A single unreadable
    /// root (permission denied, unreachable network mount, FUSE with
    /// a gone userspace process) would otherwise abort the whole scan
    /// — and since `run()` propagates via `?`, the tokio task would
    /// terminate. Instead, log a warning per failing root and keep
    /// going with the others.
    pub async fn initial_scan(&self) -> anyhow::Result<usize> {
        let dirs = self.config.expanded_directories();
        if dirs.is_empty() {
            warn!("no valid watch directories found");
            return Ok(0);
        }

        let mut count = 0;
        for dir in &dirs {
            info!("scanning {}", dir.display());
            match self.scan_directory(dir, 0).await {
                Ok(n) => count += n,
                Err(e) => warn!("scan of {} failed: {e}", dir.display()),
            }
        }
        info!("initial scan complete: {count} projects found");
        Ok(count)
    }

    /// Recursively scan a directory up to `max_depth`.
    ///
    /// Belt-and-braces error handling: every failure mode (read_dir
    /// EACCES, detection I/O, per-project registration, recursion
    /// into an unreadable subdir) is caught and logged. A single
    /// broken subdir cannot terminate the scan of its siblings —
    /// and since the caller already swallows our Err, the loop here
    /// is really just defense in depth.
    async fn scan_directory(&self, dir: &Path, depth: usize) -> anyhow::Result<usize> {
        if depth > self.config.max_depth {
            return Ok(0);
        }

        // Check for project signal at this level. If registration
        // fails (DB busy, event-bus unreachable), log and pretend
        // there was no signal — scan continues.
        if let Some(signal) = SignalDetector::detect(dir) {
            match self.handle_detection(dir, &signal).await {
                Ok(true) => return Ok(1),
                Ok(false) => return Ok(0),
                Err(e) => {
                    warn!("failed to register project at {}: {e}", dir.display());
                    return Ok(0);
                }
            }
        }

        // Recurse into subdirectories. `read_dir` EACCES is the most
        // common failure and is already handled here.
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) => {
                debug!("cannot read {}: {e}", dir.display());
                return Ok(0);
            }
        };

        let mut count = 0;
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            // `is_dir()` internally calls metadata(2) which returns
            // false (not an error) on EACCES — the entry is skipped
            // cleanly.
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            // Skip hidden dirs (except signal dirs) and common junk.
            if name.starts_with('.') && !SignalDetector::is_signal_dir(name) {
                continue;
            }
            if matches!(
                name,
                "node_modules" | "target" | "vendor" | "__pycache__" | ".cache"
            ) {
                continue;
            }
            // Per-subdir error catch: a broken subtree (stale FUSE
            // mount, sibling dir without access) must never abort
            // the sibling-list iteration.
            match Box::pin(self.scan_directory(&path, depth + 1)).await {
                Ok(n) => count += n,
                Err(e) => debug!("subscan of {} failed: {e}", path.display()),
            }
        }
        Ok(count)
    }

    /// Handle a detected signal: create or update the project.
    /// Returns `true` if a new project was created.
    async fn handle_detection(
        &self,
        dir: &Path,
        signal: &crate::project::signals::DetectionSignal,
    ) -> anyhow::Result<bool> {
        let root_path = dir.to_string_lossy().to_string();

        if let Some(existing) = self.store.get_by_root_path(&root_path).await? {
            // Already known -- update if explicit config changed.
            if signal.signal_type == SignalType::ExplicitConfig {
                self.update_from_config(existing, dir).await?;
            }
            return Ok(false);
        }

        let project = if signal.signal_type == SignalType::ExplicitConfig {
            self.create_from_config(dir)?
        } else {
            Project::new_inferred(signal.project_name.clone(), root_path, signal.confidence)
        };

        self.store.create(&project).await?;
        self.emitter.emit_created(
            &project.id.to_string(),
            &project.name,
            &project.root_path,
            project.inferred,
            project.confidence,
        );
        info!(
            "detected project: {} at {} (confidence {}%)",
            project.name, project.root_path, project.confidence
        );
        Ok(true)
    }

    /// Build a `Project` from a `.project` file.
    fn create_from_config(&self, dir: &Path) -> anyhow::Result<Project> {
        let cfg = ProjectParser::parse_file(&dir.join(".project"))?;
        let root_path = dir.to_string_lossy().to_string();

        let mut project = Project::new_explicit(cfg.project.id, cfg.project.name, root_path);
        project.description = cfg.project.description.unwrap_or_default();
        project.accent_color = cfg.appearance.accent_color.unwrap_or_default();
        project.icon = cfg.appearance.icon.unwrap_or_default();

        if cfg.project.status == "archived" {
            project.status = ProjectStatus::Archived;
            project.archived_at = Some(chrono::Utc::now());
        }
        Ok(project)
    }

    /// Reload an existing project from its `.project` file.
    async fn update_from_config(
        &self,
        mut project: Project,
        dir: &Path,
    ) -> anyhow::Result<()> {
        let cfg = match ProjectParser::parse_file(&dir.join(".project")) {
            Ok(c) => c,
            Err(e) => {
                warn!("failed to parse .project at {}: {e}", dir.display());
                return Ok(());
            }
        };

        project.name = cfg.project.name;
        project.description = cfg.project.description.unwrap_or_default();
        project.accent_color = cfg.appearance.accent_color.unwrap_or_default();
        project.icon = cfg.appearance.icon.unwrap_or_default();
        project.inferred = false;
        project.confidence = 100;

        match cfg.project.status.as_str() {
            "archived" if project.status == ProjectStatus::Active => {
                project.status = ProjectStatus::Archived;
                project.archived_at = Some(chrono::Utc::now());
            }
            "active" if project.status == ProjectStatus::Archived => {
                project.status = ProjectStatus::Active;
                project.archived_at = None;
            }
            _ => {}
        }

        self.store.update(&project).await?;
        self.emitter.emit_updated(&project.id.to_string(), &project.name);
        info!("updated project: {}", project.name);
        Ok(())
    }

    /// Handle deletion of a `.project` file.
    async fn handle_project_file_deleted(&self, dir: &Path) -> anyhow::Result<()> {
        let root_path = dir.to_string_lossy().to_string();
        let Some(mut project) = self.store.get_by_root_path(&root_path).await? else {
            return Ok(());
        };

        // Check if any other signal remains.
        if let Some(signal) = SignalDetector::detect(dir) {
            // Demote to inferred.
            project.inferred = true;
            project.confidence = signal.confidence;
            project.name = signal.project_name;
            self.store.update(&project).await?;
            self.emitter.emit_updated(&project.id.to_string(), &project.name);
            info!("demoted project to inferred: {}", project.name);
        } else {
            // No signals left -- archive.
            self.store.archive(project.id).await?;
            self.emitter.emit_archived(
                &project.id.to_string(),
                &project.name,
                &project.root_path,
            );
            info!("archived project: {}", project.name);
        }
        Ok(())
    }

    /// Start live filesystem monitoring (blocks until channel closes).
    pub async fn watch(self: Arc<Self>) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel::<Event>(256);

        let mut watcher = RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| {
                if let Ok(event) = res {
                    let _ = tx.blocking_send(event);
                }
            },
            Config::default(),
        )?;

        let dirs = self.config.expanded_directories();
        for dir in &dirs {
            match watcher.watch(dir, RecursiveMode::Recursive) {
                Ok(()) => info!("watching {}", dir.display()),
                Err(e) => warn!("failed to watch {}: {e}", dir.display()),
            }
        }

        if dirs.is_empty() {
            warn!("no directories to watch");
        }

        while let Some(event) = rx.recv().await {
            if let Err(e) = self.handle_fs_event(&event).await {
                error!("filesystem event error: {e}");
            }
        }

        // Keep watcher alive.
        drop(watcher);
        Ok(())
    }

    /// Dispatch a single filesystem event.
    async fn handle_fs_event(&self, event: &Event) -> anyhow::Result<()> {
        for path in &event.paths {
            let Some(filename) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };

            match event.kind {
                EventKind::Create(_) => {
                    if filename == ".project" || SignalDetector::is_signal_file(filename) {
                        if let Some(dir) = path.parent() {
                            let root = dir.to_string_lossy().to_string();
                            if self.store.get_by_root_path(&root).await?.is_none() {
                                if let Some(signal) = SignalDetector::detect(dir) {
                                    self.handle_detection(dir, &signal).await?;
                                }
                            }
                        }
                    }
                    if path.is_dir() && SignalDetector::is_signal_dir(filename) {
                        if let Some(dir) = path.parent() {
                            let root = dir.to_string_lossy().to_string();
                            if self.store.get_by_root_path(&root).await?.is_none() {
                                if let Some(signal) = SignalDetector::detect(dir) {
                                    self.handle_detection(dir, &signal).await?;
                                }
                            }
                        }
                    }
                }
                EventKind::Modify(_) => {
                    if filename == ".project" {
                        if let Some(dir) = path.parent() {
                            let root = dir.to_string_lossy().to_string();
                            if let Some(project) =
                                self.store.get_by_root_path(&root).await?
                            {
                                self.update_from_config(project, dir).await?;
                            }
                        }
                    }
                }
                EventKind::Remove(_) => {
                    if filename == ".project" {
                        if let Some(dir) = path.parent() {
                            self.handle_project_file_deleted(dir).await?;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Entry point: run initial scan + live watcher.
/// Designed to be spawned as a tokio task from `main.rs`.
pub async fn run(graph: GraphHandle) -> anyhow::Result<()> {
    let config = WatchConfig::load();
    let store = Arc::new(ProjectStore::new(graph));
    let watcher = Arc::new(ProjectWatcher::new(config, store));

    watcher.initial_scan().await?;
    watcher.watch().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    async fn setup(watch_dir: &Path) -> Arc<ProjectWatcher> {
        let graph_tmp = TempDir::new().unwrap();
        let graph =
            crate::graph::spawn(graph_tmp.path().join("graph").to_str().unwrap()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let config = WatchConfig {
            watch_directories: vec![watch_dir.to_string_lossy().to_string()],
            max_depth: 3,
        };
        let store = Arc::new(ProjectStore::new(graph));
        // Leak graph_tmp so it lives for the test duration.
        std::mem::forget(graph_tmp);
        Arc::new(ProjectWatcher::new(config, store))
    }

    #[tokio::test]
    async fn scan_finds_git_project() {
        let tmp = TempDir::new().unwrap();
        let project_dir = tmp.path().join("my-repo");
        fs::create_dir_all(project_dir.join(".git")).unwrap();

        let w = setup(tmp.path()).await;
        let count = w.initial_scan().await.unwrap();
        assert_eq!(count, 1);

        let p = w
            .store
            .get_by_root_path(&project_dir.to_string_lossy())
            .await
            .unwrap();
        assert!(p.is_some());
        assert!(p.unwrap().inferred);
    }

    #[tokio::test]
    async fn scan_finds_cargo_project() {
        let tmp = TempDir::new().unwrap();
        let project_dir = tmp.path().join("my-crate");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join("Cargo.toml"),
            "[package]\nname = \"cool-crate\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();

        let w = setup(tmp.path()).await;
        let count = w.initial_scan().await.unwrap();
        assert_eq!(count, 1);

        let p = w
            .store
            .get_by_root_path(&project_dir.to_string_lossy())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.name, "cool-crate");
        assert_eq!(p.confidence, 80);
    }

    #[tokio::test]
    async fn scan_finds_explicit_project() {
        let tmp = TempDir::new().unwrap();
        let project_dir = tmp.path().join("explicit");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join(".project"),
            "[project]\nname = \"My Project\"\n",
        )
        .unwrap();

        let w = setup(tmp.path()).await;
        let count = w.initial_scan().await.unwrap();
        assert_eq!(count, 1);

        let p = w
            .store
            .get_by_root_path(&project_dir.to_string_lossy())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.name, "My Project");
        assert!(!p.inferred);
        assert!(p.promoted);
    }

    #[tokio::test]
    async fn scan_respects_max_depth() {
        let tmp = TempDir::new().unwrap();
        // Depth 0: tmp/
        // Depth 1: tmp/a/
        // Depth 2: tmp/a/b/
        // Depth 3: tmp/a/b/c/
        // Depth 4: tmp/a/b/c/d/ -- beyond max_depth (3)
        let deep = tmp.path().join("a/b/c/d/deep-project");
        fs::create_dir_all(deep.join(".git")).unwrap();

        let config = WatchConfig {
            watch_directories: vec![tmp.path().to_string_lossy().to_string()],
            max_depth: 3,
        };
        let graph_tmp = TempDir::new().unwrap();
        let graph =
            crate::graph::spawn(graph_tmp.path().join("g").to_str().unwrap()).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let store = Arc::new(ProjectStore::new(graph));
        let w = Arc::new(ProjectWatcher::new(config, store));

        let count = w.initial_scan().await.unwrap();
        assert_eq!(count, 0, "project beyond max_depth should not be found");
    }

    #[tokio::test]
    async fn scan_skips_node_modules() {
        let tmp = TempDir::new().unwrap();
        let hidden = tmp.path().join("app/node_modules/dep");
        fs::create_dir_all(hidden.join(".git")).unwrap();

        // The actual app project at depth 1.
        let app = tmp.path().join("app");
        fs::write(
            app.join("package.json"),
            r#"{"name":"real-app","version":"1.0.0"}"#,
        )
        .unwrap();

        let w = setup(tmp.path()).await;
        let count = w.initial_scan().await.unwrap();
        assert_eq!(count, 1);

        // Should find real-app, not the dep inside node_modules.
        let p = w
            .store
            .get_by_root_path(&app.to_string_lossy())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.name, "real-app");
    }

    #[tokio::test]
    async fn scan_does_not_duplicate() {
        let tmp = TempDir::new().unwrap();
        let project_dir = tmp.path().join("repo");
        fs::create_dir_all(project_dir.join(".git")).unwrap();

        let w = setup(tmp.path()).await;
        w.initial_scan().await.unwrap();
        // Second scan should not create duplicate.
        let count = w.initial_scan().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn handle_project_file_deleted_demotes() {
        let tmp = TempDir::new().unwrap();
        let project_dir = tmp.path().join("proj");
        fs::create_dir_all(project_dir.join(".git")).unwrap();
        fs::write(
            project_dir.join(".project"),
            "[project]\nname = \"Explicit\"\n",
        )
        .unwrap();

        let w = setup(tmp.path()).await;
        w.initial_scan().await.unwrap();

        let p = w
            .store
            .get_by_root_path(&project_dir.to_string_lossy())
            .await
            .unwrap()
            .unwrap();
        assert!(!p.inferred);

        // Simulate .project deletion (keep .git).
        fs::remove_file(project_dir.join(".project")).unwrap();
        w.handle_project_file_deleted(&project_dir).await.unwrap();

        let p = w
            .store
            .get_by_root_path(&project_dir.to_string_lossy())
            .await
            .unwrap()
            .unwrap();
        assert!(p.inferred);
        assert_eq!(p.confidence, 90); // demoted to .git confidence
    }

    #[tokio::test]
    async fn handle_project_file_deleted_archives() {
        let tmp = TempDir::new().unwrap();
        let project_dir = tmp.path().join("proj");
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(
            project_dir.join(".project"),
            "[project]\nname = \"Gone\"\n",
        )
        .unwrap();

        let w = setup(tmp.path()).await;
        w.initial_scan().await.unwrap();

        // Remove .project -- no other signals remain.
        fs::remove_file(project_dir.join(".project")).unwrap();
        w.handle_project_file_deleted(&project_dir).await.unwrap();

        let p = w
            .store
            .get_by_root_path(&project_dir.to_string_lossy())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.status, ProjectStatus::Archived);
    }
}
