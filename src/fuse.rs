use crate::graph::GraphHandle;
use crate::utils::escape_cypher;
use anyhow::Result;
use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, Generation, INodeNo, MountOption, ReplyAttr,
    ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;

const TTL: Duration = Duration::from_secs(2);

const INO_ROOT: INodeNo = INodeNo(1);
const INO_PROJECTS: INodeNo = INodeNo(2);
const INO_LAST7: INodeNo = INodeNo(3);
const INO_RESERVED_MAX: u64 = 3;

#[derive(Debug, Clone)]
enum VPath {
    Root,
    Projects,
    Last7Days,
    Date(String),
    DateApp(String, String),
    ProjectApp(String),
    File { target: String, name: String },
}

/// Sanitize a project name for use as a FUSE directory name.
fn sanitize_dirname(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if s.is_empty() { "unnamed".to_string() } else { s }
}

pub struct TimelineFs {
    graph: GraphHandle,
    inodes: Mutex<HashMap<u64, VPath>>,
}

impl TimelineFs {
    fn new(graph: GraphHandle) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(INO_ROOT.0, VPath::Root);
        inodes.insert(INO_PROJECTS.0, VPath::Projects);
        inodes.insert(INO_LAST7.0, VPath::Last7Days);
        TimelineFs {
            graph,
            inodes: Mutex::new(inodes),
        }
    }

    fn hash_key(key: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        let ino = h.finish();
        if ino <= INO_RESERVED_MAX { ino.wrapping_add(1000) } else { ino }
    }

    fn register(&self, key: &str, vpath: VPath) -> INodeNo {
        let raw = Self::hash_key(key);
        self.inodes.lock().unwrap().insert(raw, vpath);
        INodeNo(raw)
    }

    fn lookup_vpath(&self, ino: INodeNo) -> Option<VPath> {
        self.inodes.lock().unwrap().get(&ino.0).cloned()
    }

    fn dir_attr(ino: INodeNo) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o555,
            nlink: 2,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn symlink_attr(ino: INodeNo, target_len: u64) -> FileAttr {
        FileAttr {
            ino,
            size: target_len,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Symlink,
            perm: 0o444,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn now_micros() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }

    fn micros_to_date_string(micros: i64) -> Option<String> {
        let secs = micros / 1_000_000;
        let dt = time::OffsetDateTime::from_unix_timestamp(secs).ok()?;
        Some(format!(
            "{:04}-{:02}-{:02}",
            dt.year(),
            dt.month() as u8,
            dt.day()
        ))
    }

    fn date_to_day_range(date_str: &str) -> Option<(i64, i64)> {
        let parts: Vec<&str> = date_str.split('-').collect();
        if parts.len() != 3 {
            return None;
        }
        let y: i32 = parts[0].parse().ok()?;
        let m: u8 = parts[1].parse().ok()?;
        let d: u8 = parts[2].parse().ok()?;
        let month = time::Month::try_from(m).ok()?;
        let date = time::Date::from_calendar_date(y, month, d).ok()?;
        let start = date
            .with_hms(0, 0, 0)
            .ok()?
            .assume_utc()
            .unix_timestamp()
            * 1_000_000;
        let end = start + 86_400 * 1_000_000;
        Some((start, end))
    }

    fn query_date_dirs(&self) -> Vec<String> {
        let cutoff = Self::now_micros() - 90 * 86_400 * 1_000_000;
        let cypher = format!(
            "MATCH (f:File) WHERE f.last_accessed > {cutoff} RETURN f.last_accessed"
        );
        let rows = match self.graph.query_rows_sync(cypher) {
            Ok(rs) => rs,
            Err(e) => {
                warn!("fuse: date query failed: {e}");
                return Vec::new();
            }
        };
        let mut dates: Vec<String> = rows
            .rows
            .iter()
            .filter_map(|row| Self::micros_to_date_string(row.first()?.as_i64()))
            .collect();
        dates.sort();
        dates.dedup();
        dates
    }

    fn query_apps_for_date(&self, date: &str) -> Vec<String> {
        let Some((start, end)) = Self::date_to_day_range(date) else {
            return Vec::new();
        };
        let cypher = format!(
            "MATCH (f:File)-[:ACCESSED_BY]->(a:App) \
             WHERE f.last_accessed >= {start} AND f.last_accessed < {end} \
             RETURN DISTINCT a.id"
        );
        self.query_string_column(cypher)
    }

    fn query_files_for_date_app(&self, date: &str, app_id: &str) -> Vec<String> {
        let Some((start, end)) = Self::date_to_day_range(date) else {
            return Vec::new();
        };
        let app_esc = escape_cypher(app_id);
        let cypher = format!(
            "MATCH (f:File)-[:ACCESSED_BY]->(a:App {{id: '{app_esc}'}}) \
             WHERE f.last_accessed >= {start} AND f.last_accessed < {end} \
             RETURN f.path"
        );
        self.query_string_column(cypher)
    }

    /// List active projects as (sanitized_name, project_id).
    fn query_active_projects(&self) -> Vec<(String, String)> {
        let cypher =
            "MATCH (p:Project) WHERE p.status = 'active' RETURN p.id, p.name ORDER BY p.name"
                .to_string();
        match self.graph.query_rows_sync(cypher) {
            Ok(rs) => rs
                .rows
                .iter()
                .filter_map(|row| {
                    let id = row.first()?.as_str();
                    let name = row.get(1)?.as_str();
                    if name.is_empty() {
                        None
                    } else {
                        Some((sanitize_dirname(name), id.to_string()))
                    }
                })
                .collect(),
            Err(e) => {
                warn!("fuse: project list query failed: {e}");
                Vec::new()
            }
        }
    }

    /// List file paths belonging to a project by its graph ID.
    fn query_files_for_project(&self, project_id: &str) -> Vec<String> {
        let pid_esc = escape_cypher(project_id);
        let cypher = format!(
            "MATCH (f:File)-[:FILE_PART_OF]->(p:Project {{id: '{pid_esc}'}}) RETURN f.path"
        );
        self.query_string_column(cypher)
    }

    fn query_last_7_days(&self) -> Vec<String> {
        let cutoff = Self::now_micros() - 7 * 86_400 * 1_000_000;
        let cypher = format!(
            "MATCH (f:File) WHERE f.last_accessed >= {cutoff} RETURN f.path"
        );
        self.query_string_column(cypher)
    }

    fn query_string_column(&self, cypher: String) -> Vec<String> {
        match self.graph.query_rows_sync(cypher) {
            Ok(rs) => rs
                .rows
                .iter()
                .filter_map(|row| {
                    let s = row.first()?.as_str();
                    if s.is_empty() { None } else { Some(s.to_string()) }
                })
                .collect(),
            Err(e) => {
                warn!("fuse: query failed: {e}");
                Vec::new()
            }
        }
    }

    fn basename(path: &str) -> &str {
        Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(path)
    }

    fn dedup_basenames(paths: &[String]) -> Vec<(String, String)> {
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut result = Vec::with_capacity(paths.len());
        for path in paths {
            let base = Self::basename(path).to_string();
            let count = counts.entry(base.clone()).or_insert(0);
            *count += 1;
            let display_name = if *count == 1 {
                base
            } else {
                let stem = Path::new(&base)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(&base);
                let ext = Path::new(&base)
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|e| format!(".{e}"))
                    .unwrap_or_default();
                format!("{stem}_{}{ext}", count)
            };
            result.push((display_name, path.clone()));
        }
        result
    }

    fn readdir_entries(&self, ino: INodeNo) -> Option<Vec<(INodeNo, FileType, String)>> {
        let vpath = self.lookup_vpath(ino)?;
        let mut entries = vec![
            (ino, FileType::Directory, ".".into()),
            (INO_ROOT, FileType::Directory, "..".into()),
        ];

        match vpath {
            VPath::Root => {
                entries.push((INO_PROJECTS, FileType::Directory, "projects".into()));
                entries.push((INO_LAST7, FileType::Directory, "last-7-days".into()));
                for date in self.query_date_dirs() {
                    let date_ino = self.register(&format!("date:{date}"), VPath::Date(date.clone()));
                    entries.push((date_ino, FileType::Directory, date));
                }
            }
            VPath::Date(ref date) => {
                let date = date.clone();
                for app_id in self.query_apps_for_date(&date) {
                    let app_ino = self.register(
                        &format!("date_app:{date}:{app_id}"),
                        VPath::DateApp(date.clone(), app_id.clone()),
                    );
                    entries.push((app_ino, FileType::Directory, app_id));
                }
            }
            VPath::DateApp(ref date, ref app_id) => {
                let (date, app_id) = (date.clone(), app_id.clone());
                let paths = self.query_files_for_date_app(&date, &app_id);
                self.add_file_entries(ino, &paths, &mut entries);
            }
            VPath::Projects => {
                for (dir_name, project_id) in self.query_active_projects() {
                    let p_ino = self.register(
                        &format!("project:{project_id}"),
                        VPath::ProjectApp(project_id),
                    );
                    entries.push((p_ino, FileType::Directory, dir_name));
                }
            }
            VPath::ProjectApp(ref project_id) => {
                let project_id = project_id.clone();
                let paths = self.query_files_for_project(&project_id);
                self.add_file_entries(ino, &paths, &mut entries);
            }
            VPath::Last7Days => {
                let paths = self.query_last_7_days();
                self.add_file_entries(ino, &paths, &mut entries);
            }
            VPath::File { .. } => return None,
        }

        Some(entries)
    }

    fn add_file_entries(
        &self,
        parent_ino: INodeNo,
        paths: &[String],
        entries: &mut Vec<(INodeNo, FileType, String)>,
    ) {
        for (name, target) in Self::dedup_basenames(paths) {
            let file_ino = self.register(
                &format!("file:{}:{name}", parent_ino.0),
                VPath::File { target, name: name.clone() },
            );
            entries.push((file_ino, FileType::Symlink, name));
        }
    }
}

impl Filesystem for TimelineFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => { reply.error(Errno::ENOENT); return; }
        };

        let parent_vpath = match self.lookup_vpath(parent) {
            Some(v) => v,
            None => { reply.error(Errno::ENOENT); return; }
        };

        match parent_vpath {
            VPath::Root => {
                if name_str == "projects" {
                    reply.entry(&TTL, &Self::dir_attr(INO_PROJECTS), Generation(0));
                } else if name_str == "last-7-days" {
                    reply.entry(&TTL, &Self::dir_attr(INO_LAST7), Generation(0));
                } else {
                    let ino = self.register(
                        &format!("date:{name_str}"),
                        VPath::Date(name_str.to_string()),
                    );
                    reply.entry(&TTL, &Self::dir_attr(ino), Generation(0));
                }
            }
            VPath::Date(ref date) => {
                let ino = self.register(
                    &format!("date_app:{date}:{name_str}"),
                    VPath::DateApp(date.clone(), name_str.to_string()),
                );
                reply.entry(&TTL, &Self::dir_attr(ino), Generation(0));
            }
            VPath::Projects => {
                // Find the project_id for this sanitized directory name.
                let projects = self.query_active_projects();
                if let Some((_dir_name, project_id)) =
                    projects.iter().find(|(n, _)| n == name_str)
                {
                    let ino = self.register(
                        &format!("project:{project_id}"),
                        VPath::ProjectApp(project_id.clone()),
                    );
                    reply.entry(&TTL, &Self::dir_attr(ino), Generation(0));
                } else {
                    reply.error(Errno::ENOENT);
                }
            }
            VPath::DateApp(_, _) | VPath::ProjectApp(_) | VPath::Last7Days => {
                let key = format!("file:{}:{name_str}", parent.0);
                let raw = Self::hash_key(&key);
                if let Some(VPath::File { ref target, .. }) = self.lookup_vpath(INodeNo(raw)) {
                    let attr = Self::symlink_attr(INodeNo(raw), target.len() as u64);
                    reply.entry(&TTL, &attr, Generation(0));
                } else {
                    reply.error(Errno::ENOENT);
                }
            }
            VPath::File { .. } => {
                reply.error(Errno::ENOTDIR);
            }
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.lookup_vpath(ino) {
            Some(VPath::File { ref target, .. }) => {
                reply.attr(&TTL, &Self::symlink_attr(ino, target.len() as u64));
            }
            Some(_) => {
                reply.attr(&TTL, &Self::dir_attr(ino));
            }
            None => {
                reply.error(Errno::ENOENT);
            }
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.readdir_entries(ino) {
            Some(e) => e,
            None => { reply.error(Errno::ENOENT); return; }
        };

        for (i, (entry_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*entry_ino, (i + 1) as u64, *kind, &name) {
                break;
            }
        }
        reply.ok();
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.lookup_vpath(ino) {
            Some(VPath::File { ref target, .. }) => {
                reply.data(target.as_bytes());
            }
            _ => {
                reply.error(Errno::ENOENT);
            }
        }
    }
}

/// Mount the timeline FUSE filesystem. Blocks until unmount.
pub fn mount(path: &str, graph: GraphHandle) -> Result<()> {
    if Path::new(path).exists() {
        let _ = std::process::Command::new("fusermount")
            .args(["-u", "-z", path])
            .output();
    }
    std::fs::create_dir_all(path)?;

    let fs = TimelineFs::new(graph);
    let mut config = fuser::Config::default();
    config.mount_options = vec![
        MountOption::RO,
        MountOption::FSName("lunaris-timeline".into()),
        MountOption::AutoUnmount,
    ];

    tracing::info!(path, "mounting timeline FUSE filesystem");
    fuser::mount2(fs, path, &config)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_simple() {
        assert_eq!(sanitize_dirname("my-project"), "my-project");
        assert_eq!(sanitize_dirname("my_project"), "my_project");
        assert_eq!(sanitize_dirname("v1.2.3"), "v1.2.3");
    }

    #[test]
    fn sanitize_spaces() {
        assert_eq!(sanitize_dirname("My Project"), "My-Project");
    }

    #[test]
    fn sanitize_special_chars() {
        assert_eq!(sanitize_dirname("project/name"), "project-name");
        assert_eq!(sanitize_dirname("project:name"), "project-name");
        assert_eq!(sanitize_dirname("a@b#c"), "a-b-c");
    }

    #[test]
    fn sanitize_empty() {
        assert_eq!(sanitize_dirname(""), "unnamed");
    }

    #[test]
    fn sanitize_unicode() {
        // Non-ASCII replaced with dashes.
        let s = sanitize_dirname("prüfung");
        assert!(s.contains('-'));
        assert!(s.starts_with("pr"));
    }
}
