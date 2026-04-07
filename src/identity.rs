/// App identity resolution via `/proc/{pid}/exe`.
///
/// Maps a process ID to an application identifier by reading the binary
/// path from procfs and matching it against known installation paths.
///
/// See `docs/architecture/CAPABILITY-TOKENS.md` Section 7.

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Errors from app identity resolution.
#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("process {0} not found")]
    ProcessNotFound(u32),
    #[error("cannot read exe path: {0}")]
    CannotReadExe(std::io::Error),
    #[error("unknown binary path: {0}")]
    UnknownBinary(PathBuf),
}

/// Resolve an app_id from a process ID by reading `/proc/{pid}/exe`.
pub fn app_id_from_pid(pid: u32) -> Result<String, IdentityError> {
    let exe_path = std::fs::read_link(format!("/proc/{pid}/exe")).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            IdentityError::ProcessNotFound(pid)
        } else {
            IdentityError::CannotReadExe(e)
        }
    })?;
    path_to_app_id(&exe_path)
}

/// Map a binary path to an app_id.
///
/// Resolution order:
/// 1. `/usr/lib/lunaris/apps/{app_id}/...` -> app_id
/// 2. `~/.local/share/lunaris/apps/{app_id}/...` -> app_id
/// 3. `*/lunaris-ai-daemon` or `*/lunaris-ai` -> "ai-daemon"
/// 4. `/usr/bin/lunaris-*` -> "system"
/// 5. (debug) cargo target directories -> "dev.{binary_name}"
/// 6. Error: UnknownBinary
pub fn path_to_app_id(path: &Path) -> Result<String, IdentityError> {
    let s = path.to_string_lossy();

    // System-installed apps.
    if let Some(rest) = s.strip_prefix("/usr/lib/lunaris/apps/") {
        if let Some(app_id) = rest.split('/').next() {
            if !app_id.is_empty() {
                return Ok(app_id.to_string());
            }
        }
    }

    // User-installed apps.
    if let Some(idx) = s.find("/.local/share/lunaris/apps/") {
        let rest = &s[idx + "/.local/share/lunaris/apps/".len()..];
        if let Some(app_id) = rest.split('/').next() {
            if !app_id.is_empty() {
                return Ok(app_id.to_string());
            }
        }
    }

    // AI daemon (check before generic lunaris- prefix).
    if s.ends_with("/lunaris-ai-daemon") || s.ends_with("/lunaris-ai") {
        return Ok("ai-daemon".to_string());
    }

    // System daemons.
    if s.starts_with("/usr/bin/lunaris-") {
        return Ok("system".to_string());
    }

    // Development builds (debug_assertions only).
    #[cfg(debug_assertions)]
    if s.contains("/target/debug/") || s.contains("/target/release/") {
        if let Some(name) = path.file_name() {
            return Ok(format!("dev.{}", name.to_string_lossy()));
        }
    }

    Err(IdentityError::UnknownBinary(path.to_path_buf()))
}

/// Check if a process is still alive.
pub fn process_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_id_from_path_system_app() {
        let path = PathBuf::from("/usr/lib/lunaris/apps/com.anki/bin/anki");
        assert_eq!(path_to_app_id(&path).unwrap(), "com.anki");
    }

    #[test]
    fn test_app_id_from_path_user_app() {
        let path =
            PathBuf::from("/home/user/.local/share/lunaris/apps/org.zotero/bin/zotero");
        assert_eq!(path_to_app_id(&path).unwrap(), "org.zotero");
    }

    #[test]
    fn test_app_id_from_path_ai_daemon() {
        let path = PathBuf::from("/usr/bin/lunaris-ai-daemon");
        assert_eq!(path_to_app_id(&path).unwrap(), "ai-daemon");
    }

    #[test]
    fn test_app_id_from_path_system_daemon() {
        let path = PathBuf::from("/usr/bin/lunaris-graph-daemon");
        assert_eq!(path_to_app_id(&path).unwrap(), "system");
    }

    #[test]
    fn test_app_id_from_path_unknown() {
        let path = PathBuf::from("/usr/bin/firefox");
        assert!(path_to_app_id(&path).is_err());
    }

    #[cfg(debug_assertions)]
    #[test]
    fn test_app_id_from_path_dev_build() {
        let path = PathBuf::from("/home/user/project/target/debug/my-app");
        assert_eq!(path_to_app_id(&path).unwrap(), "dev.my-app");
    }

    #[test]
    fn test_process_alive_self() {
        assert!(process_alive(std::process::id()));
    }

    #[test]
    fn test_process_alive_dead() {
        assert!(!process_alive(999_999_999));
    }

    #[test]
    fn test_app_id_from_pid_self() {
        // Our own process should resolve (in debug mode to dev.*)
        let result = app_id_from_pid(std::process::id());
        // In CI or release builds this may be UnknownBinary, so we just
        // check it doesn't panic and returns a result.
        let _ = result;
    }
}
