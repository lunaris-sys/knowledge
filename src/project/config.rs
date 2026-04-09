/// .project TOML file structure.
///
/// All sections except `[project]` are optional. Missing sections
/// get sensible defaults via `#[serde(default)]`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Root .project file structure.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProjectConfig {
    /// Required project metadata.
    pub project: ProjectBlock,

    /// Path configuration (include/exclude patterns).
    #[serde(default)]
    pub paths: PathsBlock,

    /// Git repository information.
    #[serde(default)]
    pub git: Option<GitBlock>,

    /// Issue tracker configuration.
    #[serde(default)]
    pub tracker: Option<TrackerBlock>,

    /// Focus Mode settings.
    #[serde(default)]
    pub focus: FocusBlock,

    /// AI context scoping.
    #[serde(default)]
    pub ai: AiBlock,

    /// Visual appearance overrides.
    #[serde(default)]
    pub appearance: AppearanceBlock,
}

/// `[project]` section: required metadata.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProjectBlock {
    /// Project UUID. Auto-generated (v7) if not present in the file.
    #[serde(default = "generate_uuid")]
    pub id: Uuid,

    /// Human-readable project name. Required, must not be empty.
    pub name: String,

    /// Optional longer description.
    #[serde(default)]
    pub description: Option<String>,

    /// "active" or "archived".
    #[serde(default = "default_status")]
    pub status: String,

    /// Date the project was created (YYYY-MM-DD or TOML date literal).
    #[serde(default, deserialize_with = "deserialize_optional_date")]
    pub created: Option<String>,

    /// Optional due date (YYYY-MM-DD or TOML date literal).
    #[serde(default, deserialize_with = "deserialize_optional_date")]
    pub due: Option<String>,
}

fn generate_uuid() -> Uuid {
    Uuid::now_v7()
}

/// Deserialize a date that can be either a TOML date literal or a string.
fn deserialize_optional_date<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct DateVisitor;

    impl<'de> de::Visitor<'de> for DateVisitor {
        type Value = Option<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a date string (YYYY-MM-DD) or TOML date literal")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: serde::Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_any(DateInnerVisitor)
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(Some(v.to_string()))
        }

        fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            // TOML date literals are deserialized as maps by toml crate.
            let date: toml::value::Datetime =
                de::Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))?;
            Ok(Some(date.to_string()))
        }
    }

    struct DateInnerVisitor;

    impl<'de> de::Visitor<'de> for DateInnerVisitor {
        type Value = Option<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a date string or TOML date")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(Some(v.to_string()))
        }

        fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            let date: toml::value::Datetime =
                de::Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))?;
            Ok(Some(date.to_string()))
        }
    }

    deserializer.deserialize_option(DateVisitor)
}

fn default_status() -> String {
    "active".to_string()
}

/// `[paths]` section: filesystem scope.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PathsBlock {
    /// Project root relative to the .project file. Usually ".".
    #[serde(default = "default_root")]
    pub root: String,

    /// Extra directories to include (may use ~ for home).
    #[serde(default)]
    pub include: Vec<String>,

    /// Patterns to exclude from indexing.
    #[serde(default = "default_exclude")]
    pub exclude: Vec<String>,
}

fn default_root() -> String {
    ".".to_string()
}

fn default_exclude() -> Vec<String> {
    vec![
        "node_modules".into(),
        "target".into(),
        ".git".into(),
        "dist".into(),
        "build".into(),
        "__pycache__".into(),
        "*.pyc".into(),
        ".next".into(),
        "vendor".into(),
    ]
}

impl Default for PathsBlock {
    fn default() -> Self {
        Self {
            root: default_root(),
            include: Vec::new(),
            exclude: default_exclude(),
        }
    }
}

/// `[git]` section: repository information.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GitBlock {
    /// Remote URL (HTTPS or SSH).
    pub remote: String,

    /// Default branch name.
    #[serde(default = "default_branch")]
    pub default_branch: String,
}

fn default_branch() -> String {
    "main".to_string()
}

/// `[tracker]` section: issue tracker.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TrackerBlock {
    /// Provider identifier.
    pub provider: String,

    /// Tracker URL (project board, repo issues page, etc.).
    pub url: String,
}

/// Valid tracker provider values.
pub const VALID_PROVIDERS: &[&str] = &[
    "github-issues",
    "gitlab-issues",
    "openproject",
    "plane",
    "jira",
];

/// `[focus]` section: Focus Mode behaviour.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct FocusBlock {
    /// App IDs whose notifications are suppressed in Focus Mode.
    #[serde(default)]
    pub suppress_notifications_from: Vec<String>,

    /// Modules to activate when Focus Mode enters this project.
    #[serde(default)]
    pub active_modules: Vec<String>,
}

/// `[ai]` section: AI assistant context.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AiBlock {
    /// How much context the AI sees.
    #[serde(default = "default_context_scope")]
    pub context_scope: String,

    /// Extra files always included in AI context.
    #[serde(default)]
    pub include_files: Vec<String>,
}

fn default_context_scope() -> String {
    "project".to_string()
}

/// Valid AI context scope values.
pub const VALID_CONTEXT_SCOPES: &[&str] = &["minimal", "project", "time-scoped"];

impl Default for AiBlock {
    fn default() -> Self {
        Self {
            context_scope: default_context_scope(),
            include_files: Vec::new(),
        }
    }
}

/// `[appearance]` section: visual overrides.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AppearanceBlock {
    /// Accent colour in hex (#RGB or #RRGGBB).
    #[serde(default)]
    pub accent_color: Option<String>,

    /// Lucide icon name.
    #[serde(default)]
    pub icon: Option<String>,
}
