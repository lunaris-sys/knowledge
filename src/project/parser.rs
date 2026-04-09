/// .project file parser with validation.
///
/// Reads TOML, deserializes into `ProjectConfig`, and validates
/// all fields against the allowed value sets.

use std::path::Path;
use thiserror::Error;

use crate::project::config::{ProjectConfig, VALID_CONTEXT_SCOPES, VALID_PROVIDERS};

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<toml::de::Error> for ParseError {
    fn from(e: toml::de::Error) -> Self {
        Self::InvalidToml(e)
    }
}

/// Errors from parsing or validating a .project file.
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("file not found: {0}")]
    NotFound(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("IO error: {0}")]
    Io(std::io::Error),

    #[error("invalid TOML: {0}")]
    InvalidToml(toml::de::Error),

    #[error("missing required field: {0}")]
    MissingField(String),

    #[error("invalid status: must be 'active' or 'archived', got '{0}'")]
    InvalidStatus(String),

    #[error("invalid color: must be hex format, got '{0}'")]
    InvalidColor(String),

    #[error("invalid tracker provider: '{0}'")]
    InvalidProvider(String),

    #[error("invalid AI context scope: '{0}'")]
    InvalidContextScope(String),
}

/// Parser for .project TOML files.
pub struct ProjectParser;

impl ProjectParser {
    /// Parse a .project file from disk.
    pub fn parse_file(path: &Path) -> Result<ProjectConfig, ParseError> {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                return Err(match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        ParseError::NotFound(path.display().to_string())
                    }
                    std::io::ErrorKind::PermissionDenied => {
                        ParseError::PermissionDenied(path.display().to_string())
                    }
                    _ => ParseError::Io(e),
                });
            }
        };
        Self::parse_str(&content)
    }

    /// Parse .project content from a string.
    pub fn parse_str(content: &str) -> Result<ProjectConfig, ParseError> {
        let config: ProjectConfig = toml::from_str(content)?;
        validate(&config)?;
        Ok(config)
    }
}

/// Validate all fields after deserialization.
fn validate(config: &ProjectConfig) -> Result<(), ParseError> {
    if config.project.name.trim().is_empty() {
        return Err(ParseError::MissingField("project.name".into()));
    }

    validate_status(&config.project.status)?;

    if let Some(ref color) = config.appearance.accent_color {
        validate_hex_color(color)?;
    }

    if let Some(ref tracker) = config.tracker {
        validate_provider(&tracker.provider)?;
    }

    validate_context_scope(&config.ai.context_scope)?;

    Ok(())
}

fn validate_status(status: &str) -> Result<(), ParseError> {
    match status {
        "active" | "archived" => Ok(()),
        _ => Err(ParseError::InvalidStatus(status.into())),
    }
}

fn validate_hex_color(color: &str) -> Result<(), ParseError> {
    if !color.starts_with('#') {
        return Err(ParseError::InvalidColor(color.into()));
    }
    let hex = &color[1..];
    let valid =
        (hex.len() == 3 || hex.len() == 6) && hex.chars().all(|c| c.is_ascii_hexdigit());
    if valid {
        Ok(())
    } else {
        Err(ParseError::InvalidColor(color.into()))
    }
}

fn validate_provider(provider: &str) -> Result<(), ParseError> {
    if VALID_PROVIDERS.contains(&provider) {
        Ok(())
    } else {
        Err(ParseError::InvalidProvider(provider.into()))
    }
}

fn validate_context_scope(scope: &str) -> Result<(), ParseError> {
    if VALID_CONTEXT_SCOPES.contains(&scope) {
        Ok(())
    } else {
        Err(ParseError::InvalidContextScope(scope.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal() {
        let config = ProjectParser::parse_str(
            r##"
[project]
name = "Test"
"##,
        )
        .unwrap();

        assert_eq!(config.project.name, "Test");
        assert_eq!(config.project.status, "active");
        assert!(!config.project.id.is_nil());
        assert!(!config.paths.exclude.is_empty());
        assert_eq!(config.ai.context_scope, "project");
    }

    #[test]
    fn parse_full() {
        let config = ProjectParser::parse_str(
            r##"
[project]
id = "550e8400-e29b-41d4-a716-446655440000"
name = "Full Project"
description = "A complete example"
status = "active"
created = 2026-03-15
due = 2026-06-01

[paths]
root = "."
include = ["docs/", "~/notes/"]
exclude = ["target/", "*.log"]

[git]
remote = "https://github.com/user/repo"
default_branch = "develop"

[tracker]
provider = "github-issues"
url = "https://github.com/user/repo"

[focus]
suppress_notifications_from = ["slack", "discord"]
active_modules = ["git-status"]

[ai]
context_scope = "project"
include_files = ["README.md", "CLAUDE.md"]

[appearance]
accent_color = "#6366f1"
icon = "code"
"##,
        )
        .unwrap();

        assert_eq!(
            config.project.id.to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(config.project.name, "Full Project");
        assert_eq!(
            config.project.description.as_deref(),
            Some("A complete example")
        );
        assert_eq!(config.project.created.as_deref(), Some("2026-03-15"));
        assert_eq!(config.project.due.as_deref(), Some("2026-06-01"));
        assert_eq!(config.git.as_ref().unwrap().default_branch, "develop");
        assert_eq!(
            config.tracker.as_ref().unwrap().provider,
            "github-issues"
        );
        assert_eq!(config.focus.suppress_notifications_from.len(), 2);
        assert_eq!(config.ai.include_files.len(), 2);
        assert_eq!(
            config.appearance.accent_color.as_deref(),
            Some("#6366f1")
        );
        assert_eq!(config.appearance.icon.as_deref(), Some("code"));
    }

    #[test]
    fn auto_generates_uuid() {
        let config = ProjectParser::parse_str(
            r##"
[project]
name = "No UUID"
"##,
        )
        .unwrap();
        assert!(!config.project.id.is_nil());
    }

    #[test]
    fn reject_empty_name() {
        let err = ProjectParser::parse_str(
            r##"
[project]
name = ""
"##,
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::MissingField(_)));
    }

    #[test]
    fn reject_whitespace_name() {
        let err = ProjectParser::parse_str(
            r##"
[project]
name = "   "
"##,
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::MissingField(_)));
    }

    #[test]
    fn reject_invalid_status() {
        let err = ProjectParser::parse_str(
            r##"
[project]
name = "Test"
status = "paused"
"##,
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidStatus(ref s) if s == "paused")
        );
    }

    #[test]
    fn reject_color_no_hash() {
        let err = ProjectParser::parse_str(
            r##"
[project]
name = "Test"

[appearance]
accent_color = "6366f1"
"##,
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::InvalidColor(_)));
    }

    #[test]
    fn reject_color_wrong_length() {
        let err = ProjectParser::parse_str(
            r##"
[project]
name = "Test"

[appearance]
accent_color = "#6366"
"##,
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::InvalidColor(_)));
    }

    #[test]
    fn reject_color_non_hex() {
        let err = ProjectParser::parse_str(
            r##"
[project]
name = "Test"

[appearance]
accent_color = "#gggggg"
"##,
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::InvalidColor(_)));
    }

    #[test]
    fn accept_valid_colors() {
        for color in ["#fff", "#FFF", "#ffffff", "#FFFFFF", "#6366f1", "#ABC"] {
            let content = format!(
                "[project]\nname = \"Test\"\n\n[appearance]\naccent_color = \"{color}\"\n"
            );
            assert!(
                ProjectParser::parse_str(&content).is_ok(),
                "color {color} should be valid"
            );
        }
    }

    #[test]
    fn reject_invalid_provider() {
        let err = ProjectParser::parse_str(
            r##"
[project]
name = "Test"

[tracker]
provider = "trello"
url = "https://trello.com/board"
"##,
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidProvider(ref p) if p == "trello")
        );
    }

    #[test]
    fn accept_valid_providers() {
        for provider in ["github-issues", "gitlab-issues", "openproject", "plane", "jira"] {
            let content = format!(
                "[project]\nname = \"Test\"\n\n[tracker]\nprovider = \"{provider}\"\nurl = \"https://example.com\"\n"
            );
            assert!(
                ProjectParser::parse_str(&content).is_ok(),
                "provider {provider} should be valid"
            );
        }
    }

    #[test]
    fn reject_invalid_context_scope() {
        let err = ProjectParser::parse_str(
            r##"
[project]
name = "Test"

[ai]
context_scope = "global"
"##,
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::InvalidContextScope(ref s) if s == "global")
        );
    }

    #[test]
    fn accept_valid_context_scopes() {
        for scope in ["minimal", "project", "time-scoped"] {
            let content = format!(
                "[project]\nname = \"Test\"\n\n[ai]\ncontext_scope = \"{scope}\"\n"
            );
            assert!(
                ProjectParser::parse_str(&content).is_ok(),
                "scope {scope} should be valid"
            );
        }
    }

    #[test]
    fn defaults_for_missing_sections() {
        let config = ProjectParser::parse_str(
            r##"
[project]
name = "Minimal"
"##,
        )
        .unwrap();

        assert_eq!(config.paths.root, ".");
        assert!(config.paths.include.is_empty());
        assert!(config.paths.exclude.contains(&"node_modules".to_string()));
        assert!(config.focus.suppress_notifications_from.is_empty());
        assert!(config.focus.active_modules.is_empty());
        assert_eq!(config.ai.context_scope, "project");
        assert!(config.ai.include_files.is_empty());
        assert!(config.git.is_none());
        assert!(config.tracker.is_none());
        assert!(config.appearance.accent_color.is_none());
        assert!(config.appearance.icon.is_none());
    }

    #[test]
    fn invalid_toml_syntax() {
        let err = ProjectParser::parse_str("[project\nname = \"Broken\"\n").unwrap_err();
        assert!(matches!(err, ParseError::InvalidToml(_)));
    }

    #[test]
    fn missing_project_section() {
        let err = ProjectParser::parse_str("[paths]\nroot = \".\"\n").unwrap_err();
        assert!(matches!(err, ParseError::InvalidToml(_)));
    }

    #[test]
    fn parse_file_not_found() {
        let err =
            ProjectParser::parse_file(Path::new("/nonexistent/.project")).unwrap_err();
        assert!(matches!(err, ParseError::NotFound(_)));
    }

    #[test]
    fn git_default_branch() {
        let config = ProjectParser::parse_str(
            r##"
[project]
name = "Test"

[git]
remote = "https://github.com/user/repo"
"##,
        )
        .unwrap();
        assert_eq!(config.git.as_ref().unwrap().default_branch, "main");
    }

    #[test]
    fn roundtrip_serialize() {
        let config = ProjectParser::parse_str(
            r##"
[project]
id = "550e8400-e29b-41d4-a716-446655440000"
name = "Roundtrip"
status = "active"
"##,
        )
        .unwrap();

        let serialized = toml::to_string(&config).unwrap();
        let reparsed = ProjectParser::parse_str(&serialized).unwrap();
        assert_eq!(reparsed.project.name, "Roundtrip");
        assert_eq!(
            reparsed.project.id.to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }
}
