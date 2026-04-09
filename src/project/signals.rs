/// Project signal detection.
///
/// Scans a directory for files/directories that indicate it is a
/// project root (`.project`, `.git`, `Cargo.toml`, etc.) and returns
/// the highest-confidence match.

use std::path::Path;
use std::process::Command;

/// A detected project signal with confidence and inferred name.
#[derive(Debug, Clone)]
pub struct DetectionSignal {
    pub signal_type: SignalType,
    pub confidence: u8,
    pub project_name: String,
}

/// The kind of file or directory that was detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    ExplicitConfig, // .project   100%
    Git,            // .git       90%
    CargoToml,      // Cargo.toml 80%
    PackageJson,    // package.json 80%
    PyProject,      // pyproject.toml 80%
    GoMod,          // go.mod     80%
    Solution,       // *.sln      80%
    PomXml,         // pom.xml    80%
    BuildGradle,    // build.gradle 70%
    IdeaDir,        // .idea/     70%
    VscodeDir,      // .vscode/   60%
    Makefile,       // Makefile   50%
}

impl SignalType {
    /// Confidence percentage for this signal type.
    pub fn confidence(&self) -> u8 {
        match self {
            Self::ExplicitConfig => 100,
            Self::Git => 90,
            Self::CargoToml
            | Self::PackageJson
            | Self::PyProject
            | Self::GoMod
            | Self::Solution
            | Self::PomXml => 80,
            Self::BuildGradle | Self::IdeaDir => 70,
            Self::VscodeDir => 60,
            Self::Makefile => 50,
        }
    }
}

/// Detects project signals in a directory.
pub struct SignalDetector;

impl SignalDetector {
    /// Return the highest-confidence signal in `dir`, or `None`.
    pub fn detect(dir: &Path) -> Option<DetectionSignal> {
        if dir.join(".project").exists() {
            return Some(DetectionSignal {
                signal_type: SignalType::ExplicitConfig,
                confidence: 100,
                project_name: Self::name_from_project_file(dir),
            });
        }

        // .git can be a file (worktrees) or directory.
        if dir.join(".git").exists() {
            return Some(DetectionSignal {
                signal_type: SignalType::Git,
                confidence: 90,
                project_name: Self::name_from_git(dir),
            });
        }

        if let Some(name) = Self::name_from_cargo_toml(dir) {
            return Some(DetectionSignal {
                signal_type: SignalType::CargoToml,
                confidence: 80,
                project_name: name,
            });
        }

        if let Some(name) = Self::name_from_package_json(dir) {
            return Some(DetectionSignal {
                signal_type: SignalType::PackageJson,
                confidence: 80,
                project_name: name,
            });
        }

        if let Some(name) = Self::name_from_pyproject(dir) {
            return Some(DetectionSignal {
                signal_type: SignalType::PyProject,
                confidence: 80,
                project_name: name,
            });
        }

        if let Some(name) = Self::name_from_go_mod(dir) {
            return Some(DetectionSignal {
                signal_type: SignalType::GoMod,
                confidence: 80,
                project_name: name,
            });
        }

        if let Some(name) = Self::name_from_sln(dir) {
            return Some(DetectionSignal {
                signal_type: SignalType::Solution,
                confidence: 80,
                project_name: name,
            });
        }

        if let Some(name) = Self::name_from_pom_xml(dir) {
            return Some(DetectionSignal {
                signal_type: SignalType::PomXml,
                confidence: 80,
                project_name: name,
            });
        }

        if dir.join("build.gradle").exists() || dir.join("build.gradle.kts").exists() {
            return Some(DetectionSignal {
                signal_type: SignalType::BuildGradle,
                confidence: 70,
                project_name: Self::dir_name(dir),
            });
        }

        if dir.join(".idea").is_dir() {
            return Some(DetectionSignal {
                signal_type: SignalType::IdeaDir,
                confidence: 70,
                project_name: Self::dir_name(dir),
            });
        }

        if dir.join(".vscode").is_dir() {
            return Some(DetectionSignal {
                signal_type: SignalType::VscodeDir,
                confidence: 60,
                project_name: Self::dir_name(dir),
            });
        }

        if dir.join("Makefile").exists() || dir.join("makefile").exists() {
            return Some(DetectionSignal {
                signal_type: SignalType::Makefile,
                confidence: 50,
                project_name: Self::dir_name(dir),
            });
        }

        None
    }

    /// Whether a filename is a project signal (for watcher events).
    pub fn is_signal_file(filename: &str) -> bool {
        matches!(
            filename,
            ".project"
                | "Cargo.toml"
                | "package.json"
                | "pyproject.toml"
                | "go.mod"
                | "pom.xml"
                | "build.gradle"
                | "build.gradle.kts"
                | "Makefile"
                | "makefile"
        ) || filename.ends_with(".sln")
    }

    /// Whether a directory name is a project signal.
    pub fn is_signal_dir(dirname: &str) -> bool {
        matches!(dirname, ".git" | ".idea" | ".vscode")
    }

    // ── Name extraction helpers ─────────────────────────────────────────

    fn dir_name(dir: &Path) -> String {
        dir.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    fn name_from_project_file(dir: &Path) -> String {
        std::fs::read_to_string(dir.join(".project"))
            .ok()
            .and_then(|c| crate::project::ProjectParser::parse_str(&c).ok())
            .map(|cfg| cfg.project.name)
            .unwrap_or_else(|| Self::dir_name(dir))
    }

    fn name_from_git(dir: &Path) -> String {
        if let Ok(out) = Command::new("git")
            .args(["-C", &dir.to_string_lossy(), "remote", "get-url", "origin"])
            .output()
        {
            if out.status.success() {
                let url = String::from_utf8_lossy(&out.stdout);
                if let Some(name) = Self::repo_name_from_url(url.trim()) {
                    return name;
                }
            }
        }
        Self::dir_name(dir)
    }

    fn repo_name_from_url(url: &str) -> Option<String> {
        let url = url.trim().trim_end_matches(".git");
        url.rsplit(['/', ':'])
            .next()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    }

    fn name_from_cargo_toml(dir: &Path) -> Option<String> {
        let content = std::fs::read_to_string(dir.join("Cargo.toml")).ok()?;
        let doc: toml::Value = toml::from_str(&content).ok()?;
        doc.get("package")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string())
    }

    fn name_from_package_json(dir: &Path) -> Option<String> {
        let content = std::fs::read_to_string(dir.join("package.json")).ok()?;
        let doc: serde_json::Value = serde_json::from_str(&content).ok()?;
        doc.get("name")
            .and_then(|n| n.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    }

    fn name_from_pyproject(dir: &Path) -> Option<String> {
        let content = std::fs::read_to_string(dir.join("pyproject.toml")).ok()?;
        let doc: toml::Value = toml::from_str(&content).ok()?;
        doc.get("project")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .or_else(|| {
                doc.get("tool")
                    .and_then(|t| t.get("poetry"))
                    .and_then(|p| p.get("name"))
                    .and_then(|n| n.as_str())
            })
            .map(|s| s.to_string())
    }

    fn name_from_go_mod(dir: &Path) -> Option<String> {
        let content = std::fs::read_to_string(dir.join("go.mod")).ok()?;
        let first = content.lines().next()?;
        let module_path = first.strip_prefix("module ")?.trim();
        module_path.rsplit('/').next().map(|s| s.to_string())
    }

    fn name_from_sln(dir: &Path) -> Option<String> {
        for entry in std::fs::read_dir(dir).ok()?.filter_map(|e| e.ok()) {
            let p = entry.path();
            if p.extension().map(|e| e == "sln").unwrap_or(false) {
                return p.file_stem().and_then(|s| s.to_str()).map(|s| s.to_string());
            }
        }
        None
    }

    fn name_from_pom_xml(dir: &Path) -> Option<String> {
        let content = std::fs::read_to_string(dir.join("pom.xml")).ok()?;
        let re = regex::Regex::new(
            r"(?s)<project[^>]*>.*?<artifactId>([^<]+)</artifactId>",
        )
        .ok()?;
        re.captures(&content)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn repo_url_https() {
        assert_eq!(
            SignalDetector::repo_name_from_url("https://github.com/user/repo.git"),
            Some("repo".into())
        );
    }

    #[test]
    fn repo_url_ssh() {
        assert_eq!(
            SignalDetector::repo_name_from_url("git@github.com:user/repo.git"),
            Some("repo".into())
        );
    }

    #[test]
    fn repo_url_no_suffix() {
        assert_eq!(
            SignalDetector::repo_name_from_url("https://github.com/user/repo"),
            Some("repo".into())
        );
    }

    #[test]
    fn is_signal_file_positive() {
        for f in [
            ".project",
            "Cargo.toml",
            "package.json",
            "pyproject.toml",
            "go.mod",
            "pom.xml",
            "build.gradle",
            "Makefile",
            "MyApp.sln",
        ] {
            assert!(SignalDetector::is_signal_file(f), "{f} should be signal");
        }
    }

    #[test]
    fn is_signal_file_negative() {
        for f in ["README.md", "main.rs", "index.ts"] {
            assert!(!SignalDetector::is_signal_file(f), "{f} should not be signal");
        }
    }

    #[test]
    fn is_signal_dir_positive() {
        for d in [".git", ".idea", ".vscode"] {
            assert!(SignalDetector::is_signal_dir(d));
        }
    }

    #[test]
    fn is_signal_dir_negative() {
        assert!(!SignalDetector::is_signal_dir("src"));
    }

    #[test]
    fn confidence_order() {
        assert!(SignalType::ExplicitConfig.confidence() > SignalType::Git.confidence());
        assert!(SignalType::Git.confidence() > SignalType::CargoToml.confidence());
        assert!(SignalType::CargoToml.confidence() > SignalType::BuildGradle.confidence());
        assert!(SignalType::BuildGradle.confidence() > SignalType::Makefile.confidence());
    }

    #[test]
    fn detect_git() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir(tmp.path().join(".git")).unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::Git);
        assert_eq!(sig.confidence, 90);
    }

    #[test]
    fn detect_cargo() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("Cargo.toml"),
            "[package]\nname = \"my-crate\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::CargoToml);
        assert_eq!(sig.project_name, "my-crate");
    }

    #[test]
    fn detect_npm() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("package.json"),
            r#"{"name":"my-app","version":"1.0.0"}"#,
        )
        .unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::PackageJson);
        assert_eq!(sig.project_name, "my-app");
    }

    #[test]
    fn detect_python() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("pyproject.toml"),
            "[project]\nname = \"my-py\"\n",
        )
        .unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::PyProject);
        assert_eq!(sig.project_name, "my-py");
    }

    #[test]
    fn detect_poetry() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("pyproject.toml"),
            "[tool.poetry]\nname = \"poetry-app\"\n",
        )
        .unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.project_name, "poetry-app");
    }

    #[test]
    fn detect_go() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("go.mod"),
            "module github.com/user/myapp\n\ngo 1.21\n",
        )
        .unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::GoMod);
        assert_eq!(sig.project_name, "myapp");
    }

    #[test]
    fn detect_gradle() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("build.gradle"), "").unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::BuildGradle);
    }

    #[test]
    fn detect_idea() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir(tmp.path().join(".idea")).unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::IdeaDir);
    }

    #[test]
    fn detect_vscode() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir(tmp.path().join(".vscode")).unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::VscodeDir);
    }

    #[test]
    fn detect_makefile() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("Makefile"), "all:\n\techo hi\n").unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::Makefile);
    }

    #[test]
    fn explicit_beats_all() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir(tmp.path().join(".git")).unwrap();
        fs::write(
            tmp.path().join("Cargo.toml"),
            "[package]\nname = \"cargo\"\n",
        )
        .unwrap();
        fs::write(
            tmp.path().join(".project"),
            "[project]\nname = \"explicit\"\n",
        )
        .unwrap();
        let sig = SignalDetector::detect(tmp.path()).unwrap();
        assert_eq!(sig.signal_type, SignalType::ExplicitConfig);
        assert_eq!(sig.project_name, "explicit");
    }

    #[test]
    fn no_signal_returns_none() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("README.md"), "# hello\n").unwrap();
        assert!(SignalDetector::detect(tmp.path()).is_none());
    }
}
