/// Watch configuration for project detection.
///
/// Loaded from `~/.config/lunaris/graph.toml` `[projects]` section.
/// Falls back to defaults if the file is missing or unparseable.

use serde::Deserialize;
use std::path::PathBuf;

/// `[projects]` section from `graph.toml`.
#[derive(Debug, Clone, Deserialize)]
pub struct WatchConfig {
    /// Directories to scan for projects (supports `~`).
    #[serde(default = "default_watch_dirs")]
    pub watch_directories: Vec<String>,

    /// Maximum recursion depth when scanning.
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,

    /// Auto-promote an inferred project after this many distinct
    /// files have been opened in one session. Lower = faster
    /// promotion, more noise; higher = slower, only well-used
    /// projects surface in Waypointer / Focus Mode. Was a
    /// hardcoded `3` until Sprint C made it user-configurable.
    #[serde(default = "default_auto_promote_threshold")]
    pub auto_promote_threshold: usize,
}

fn default_watch_dirs() -> Vec<String> {
    vec![
        "~/Projects".into(),
        "~/Repositories".into(),
        "~/Documents".into(),
        "~/Developer".into(),
        "~/Code".into(),
    ]
}

fn default_max_depth() -> usize {
    3
}

fn default_auto_promote_threshold() -> usize {
    3
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            watch_directories: default_watch_dirs(),
            max_depth: default_max_depth(),
            auto_promote_threshold: default_auto_promote_threshold(),
        }
    }
}

/// Top-level `graph.toml` structure.
#[derive(Debug, Clone, Default, Deserialize)]
struct GraphConfig {
    #[serde(default)]
    projects: WatchConfig,
}

impl WatchConfig {
    /// Load from `~/.config/lunaris/graph.toml`.
    /// Returns defaults if the file is missing or invalid.
    pub fn load() -> Self {
        let Some(path) = dirs::config_dir().map(|p| p.join("lunaris/graph.toml")) else {
            tracing::debug!("no config dir found, using defaults");
            return Self::default();
        };

        if !path.exists() {
            tracing::debug!("{} not found, using defaults", path.display());
            return Self::default();
        }

        match std::fs::read_to_string(&path) {
            Ok(content) => match toml::from_str::<GraphConfig>(&content) {
                Ok(gc) => {
                    tracing::info!("loaded project config from {}", path.display());
                    gc.projects
                }
                Err(e) => {
                    tracing::warn!("failed to parse {}: {e}, using defaults", path.display());
                    Self::default()
                }
            },
            Err(e) => {
                tracing::warn!("failed to read {}: {e}, using defaults", path.display());
                Self::default()
            }
        }
    }

    /// Expand `~` and filter to existing directories.
    pub fn expanded_directories(&self) -> Vec<PathBuf> {
        self.watch_directories
            .iter()
            .filter_map(|dir| {
                let expanded = shellexpand::tilde(dir);
                let path = PathBuf::from(expanded.as_ref());
                if path.is_dir() {
                    Some(path)
                } else {
                    tracing::debug!("watch directory does not exist: {dir}");
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_has_entries() {
        let cfg = WatchConfig::default();
        assert!(!cfg.watch_directories.is_empty());
        assert_eq!(cfg.max_depth, 3);
    }

    #[test]
    fn parse_custom_config() {
        let toml = r#"
[projects]
watch_directories = ["/tmp/projects"]
max_depth = 2
"#;
        let gc: GraphConfig = toml::from_str(toml).unwrap();
        assert_eq!(gc.projects.watch_directories, vec!["/tmp/projects"]);
        assert_eq!(gc.projects.max_depth, 2);
    }

    #[test]
    fn parse_empty_config_uses_defaults() {
        let gc: GraphConfig = toml::from_str("").unwrap();
        assert!(!gc.projects.watch_directories.is_empty());
        assert_eq!(gc.projects.max_depth, 3);
    }

    /// Sprint C added `auto_promote_threshold`. Existing user
    /// graph.toml files without the field must still parse and
    /// fall back to 3 — otherwise upgrading would crash the
    /// daemon on first start.
    #[test]
    fn missing_threshold_falls_back_to_default() {
        let toml = r#"
[projects]
watch_directories = ["/tmp/projects"]
max_depth = 2
"#;
        let gc: GraphConfig = toml::from_str(toml).unwrap();
        assert_eq!(
            gc.projects.auto_promote_threshold, 3,
            "missing threshold must default to 3 (compositor #29 era \
             behaviour) — change with care, this affects every existing \
             user's graph.toml"
        );
    }

    #[test]
    fn explicit_threshold_overrides_default() {
        let toml = r#"
[projects]
auto_promote_threshold = 7
"#;
        let gc: GraphConfig = toml::from_str(toml).unwrap();
        assert_eq!(gc.projects.auto_promote_threshold, 7);
    }
}
