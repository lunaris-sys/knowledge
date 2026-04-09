/// Project CRUD and PART_OF edge operations against the Ladybug graph.

use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;

use crate::graph::{CellValue, GraphHandle, RowSet};
use crate::utils::escape_cypher;

// ── Types ───────────────────────────────────────────────────────────────

/// Project status in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectStatus {
    /// Project is active and visible.
    Active,
    /// Project has been archived (directory removed or .project deleted).
    Archived,
}

impl ProjectStatus {
    /// Status as stored in the graph.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }

    /// Parse status from graph value.
    pub fn from_str(s: &str) -> Self {
        match s {
            "archived" => Self::Archived,
            _ => Self::Active,
        }
    }
}

/// A project entity in the Knowledge Graph.
#[derive(Debug, Clone)]
pub struct Project {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub root_path: String,
    pub accent_color: String,
    pub icon: String,
    pub status: ProjectStatus,
    pub created_at: DateTime<Utc>,
    pub last_accessed: Option<DateTime<Utc>>,
    pub inferred: bool,
    pub confidence: u8,
    pub promoted: bool,
    pub archived_at: Option<DateTime<Utc>>,
}

impl Project {
    /// Create a new inferred project from auto-detection.
    pub fn new_inferred(name: String, root_path: String, confidence: u8) -> Self {
        Self {
            id: Uuid::now_v7(),
            name,
            description: String::new(),
            root_path,
            accent_color: String::new(),
            icon: String::new(),
            status: ProjectStatus::Active,
            created_at: Utc::now(),
            last_accessed: None,
            inferred: true,
            confidence,
            promoted: false,
            archived_at: None,
        }
    }

    /// Create a new explicit project from a .project file.
    pub fn new_explicit(id: Uuid, name: String, root_path: String) -> Self {
        Self {
            id,
            name,
            description: String::new(),
            root_path,
            accent_color: String::new(),
            icon: String::new(),
            status: ProjectStatus::Active,
            created_at: Utc::now(),
            last_accessed: None,
            inferred: false,
            confidence: 100,
            promoted: true,
            archived_at: None,
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Convert DateTime<Utc> to milliseconds since epoch for graph storage.
fn dt_to_millis(dt: &DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

/// Convert millis to Option<DateTime<Utc>>. Returns None for 0.
fn millis_to_dt(millis: i64) -> Option<DateTime<Utc>> {
    if millis == 0 {
        None
    } else {
        Utc.timestamp_millis_opt(millis).single()
    }
}

/// Parse a Project from a RowSet where columns match the SELECT order.
fn parse_project(rs: &RowSet, row_idx: usize) -> Option<Project> {
    let row = rs.rows.get(row_idx)?;
    if row.len() < 13 {
        return None;
    }
    let col = |name: &str| -> usize {
        rs.columns.iter().position(|c| c == name).unwrap_or(usize::MAX)
    };
    let s = |i: usize| -> String {
        row.get(i).map(|v| v.as_str().to_string()).unwrap_or_default()
    };
    let i = |i: usize| -> i64 {
        row.get(i).map(|v| v.as_i64()).unwrap_or(0)
    };
    let b = |i: usize| -> bool {
        row.get(i)
            .map(|v| match v {
                CellValue::Bool(b) => *b,
                CellValue::Int64(n) => *n != 0,
                _ => false,
            })
            .unwrap_or(false)
    };

    let id_idx = col("p.id");
    let id_str = s(id_idx);
    let id = Uuid::parse_str(&id_str).ok()?;
    let created_at_ms = i(col("p.created_at"));

    Some(Project {
        id,
        name: s(col("p.name")),
        description: s(col("p.description")),
        root_path: s(col("p.root_path")),
        accent_color: s(col("p.accent_color")),
        icon: s(col("p.icon")),
        status: ProjectStatus::from_str(&s(col("p.status"))),
        created_at: millis_to_dt(created_at_ms).unwrap_or_else(Utc::now),
        last_accessed: millis_to_dt(i(col("p.last_accessed"))),
        inferred: b(col("p.inferred")),
        confidence: i(col("p.confidence")) as u8,
        promoted: b(col("p.promoted")),
        archived_at: millis_to_dt(i(col("p.archived_at"))),
    })
}

/// Column list for SELECT queries.
const PROJECT_COLUMNS: &str = "p.id, p.name, p.description, p.root_path, \
    p.accent_color, p.icon, p.status, p.created_at, p.last_accessed, \
    p.inferred, p.confidence, p.promoted, p.archived_at";

// ── ProjectStore ────────────────────────────────────────────────────────

/// Store for Project CRUD and PART_OF edge operations.
pub struct ProjectStore {
    graph: GraphHandle,
}

impl ProjectStore {
    /// Create a new ProjectStore.
    pub fn new(graph: GraphHandle) -> Self {
        Self { graph }
    }

    /// Insert a new project node.
    pub async fn create(&self, project: &Project) -> Result<()> {
        let id = escape_cypher(&project.id.to_string());
        let name = escape_cypher(&project.name);
        let desc = escape_cypher(&project.description);
        let root = escape_cypher(&project.root_path);
        let color = escape_cypher(&project.accent_color);
        let icon = escape_cypher(&project.icon);
        let status = project.status.as_str();
        let created = dt_to_millis(&project.created_at);
        let accessed = project.last_accessed.map(|d| dt_to_millis(&d)).unwrap_or(0);
        let inferred = project.inferred;
        let confidence = project.confidence as i64;
        let promoted = project.promoted;

        // Check for duplicate root_path first.
        let check = self.get_by_root_path(&project.root_path).await?;
        if let Some(existing) = check {
            if existing.id != project.id {
                return Err(anyhow!(
                    "project with root_path '{}' already exists (id: {})",
                    project.root_path,
                    existing.id
                ));
            }
        }

        self.graph
            .write(format!(
                "CREATE (p:Project {{
                    id: '{id}',
                    name: '{name}',
                    description: '{desc}',
                    root_path: '{root}',
                    accent_color: '{color}',
                    icon: '{icon}',
                    status: '{status}',
                    created_at: {created},
                    last_accessed: {accessed},
                    inferred: {inferred},
                    confidence: {confidence},
                    promoted: {promoted},
                    archived_at: 0
                }})"
            ))
            .await?;

        Ok(())
    }

    /// Get a project by its UUID.
    pub async fn get_by_id(&self, id: Uuid) -> Result<Option<Project>> {
        let id_esc = escape_cypher(&id.to_string());
        let rs = self
            .graph
            .query_rows(format!(
                "MATCH (p:Project {{id: '{id_esc}'}}) RETURN {PROJECT_COLUMNS}"
            ))
            .await?;
        if rs.rows.is_empty() {
            return Ok(None);
        }
        Ok(parse_project(&rs, 0))
    }

    /// Get a project by its exact root_path.
    pub async fn get_by_root_path(&self, path: &str) -> Result<Option<Project>> {
        let path_esc = escape_cypher(path);
        let rs = self
            .graph
            .query_rows(format!(
                "MATCH (p:Project {{root_path: '{path_esc}'}}) RETURN {PROJECT_COLUMNS}"
            ))
            .await?;
        if rs.rows.is_empty() {
            return Ok(None);
        }
        Ok(parse_project(&rs, 0))
    }

    /// Find the project whose root_path is the longest prefix of `file_path`.
    ///
    /// This implements the "nearest ancestor" rule: if a file lives inside
    /// nested projects, the innermost (longest path) project wins.
    pub async fn find_by_path_prefix(&self, file_path: &str) -> Result<Option<Project>> {
        let fp_esc = escape_cypher(file_path);
        let rs = self
            .graph
            .query_rows(format!(
                "MATCH (p:Project)
                 WHERE p.status = 'active'
                   AND starts_with('{fp_esc}', p.root_path)
                 RETURN {PROJECT_COLUMNS}
                 ORDER BY size(p.root_path) DESC
                 LIMIT 1"
            ))
            .await?;
        if rs.rows.is_empty() {
            return Ok(None);
        }
        Ok(parse_project(&rs, 0))
    }

    /// List all active projects.
    pub async fn list_active(&self) -> Result<Vec<Project>> {
        let rs = self
            .graph
            .query_rows(format!(
                "MATCH (p:Project) WHERE p.status = 'active' RETURN {PROJECT_COLUMNS}"
            ))
            .await?;
        Ok((0..rs.rows.len())
            .filter_map(|i| parse_project(&rs, i))
            .collect())
    }

    /// List promoted projects (visible in Waypointer and Focus Mode).
    pub async fn list_promoted(&self) -> Result<Vec<Project>> {
        let rs = self
            .graph
            .query_rows(format!(
                "MATCH (p:Project)
                 WHERE p.promoted = true AND p.status = 'active'
                 RETURN {PROJECT_COLUMNS}"
            ))
            .await?;
        Ok((0..rs.rows.len())
            .filter_map(|i| parse_project(&rs, i))
            .collect())
    }

    /// Update a project's mutable fields.
    pub async fn update(&self, project: &Project) -> Result<()> {
        let id = escape_cypher(&project.id.to_string());
        let name = escape_cypher(&project.name);
        let desc = escape_cypher(&project.description);
        let color = escape_cypher(&project.accent_color);
        let icon = escape_cypher(&project.icon);
        let status = project.status.as_str();
        let inferred = project.inferred;
        let confidence = project.confidence as i64;
        let promoted = project.promoted;

        self.graph
            .write(format!(
                "MATCH (p:Project {{id: '{id}'}})
                 SET p.name = '{name}',
                     p.description = '{desc}',
                     p.accent_color = '{color}',
                     p.icon = '{icon}',
                     p.status = '{status}',
                     p.inferred = {inferred},
                     p.confidence = {confidence},
                     p.promoted = {promoted}"
            ))
            .await?;
        Ok(())
    }

    /// Archive a project (soft delete).
    pub async fn archive(&self, id: Uuid) -> Result<()> {
        let id_esc = escape_cypher(&id.to_string());
        let now = Utc::now().timestamp_millis();
        self.graph
            .write(format!(
                "MATCH (p:Project {{id: '{id_esc}'}})
                 SET p.status = 'archived', p.archived_at = {now}"
            ))
            .await?;
        Ok(())
    }

    /// Delete a project node and all its edges.
    pub async fn delete(&self, id: Uuid) -> Result<()> {
        let id_esc = escape_cypher(&id.to_string());
        self.graph
            .write(format!(
                "MATCH (p:Project {{id: '{id_esc}'}}) DETACH DELETE p"
            ))
            .await?;
        Ok(())
    }

    /// Promote a project (make visible in Waypointer).
    pub async fn promote(&self, id: Uuid) -> Result<()> {
        let id_esc = escape_cypher(&id.to_string());
        self.graph
            .write(format!(
                "MATCH (p:Project {{id: '{id_esc}'}}) SET p.promoted = true"
            ))
            .await?;
        Ok(())
    }

    /// Update the last_accessed timestamp.
    pub async fn touch(&self, id: Uuid) -> Result<()> {
        let id_esc = escape_cypher(&id.to_string());
        let now = Utc::now().timestamp_millis();
        self.graph
            .write(format!(
                "MATCH (p:Project {{id: '{id_esc}'}}) SET p.last_accessed = {now}"
            ))
            .await?;
        Ok(())
    }

    /// Check if the project's root_path still exists on disk.
    pub async fn validate_path(&self, id: Uuid) -> Result<bool> {
        if let Some(project) = self.get_by_id(id).await? {
            Ok(std::path::Path::new(&project.root_path).exists())
        } else {
            Ok(false)
        }
    }

    // ── PART_OF Edge Operations ─────────────────────────────────────────

    /// Create a FILE_PART_OF edge from a File node to a Project node.
    pub async fn link_file(&self, file_id: &str, project_id: Uuid) -> Result<()> {
        let fid = escape_cypher(file_id);
        let pid = escape_cypher(&project_id.to_string());
        self.graph
            .write(format!(
                "MATCH (f:File {{id: '{fid}'}}), (p:Project {{id: '{pid}'}})
                 MERGE (f)-[:FILE_PART_OF]->(p)"
            ))
            .await?;
        Ok(())
    }

    /// Check if a file is already linked to a project.
    pub async fn is_file_linked(&self, file_id: &str, project_id: Uuid) -> Result<bool> {
        let fid = escape_cypher(file_id);
        let pid = escape_cypher(&project_id.to_string());
        let rs = self
            .graph
            .query_rows(format!(
                "MATCH (f:File {{id: '{fid}'}})-[:FILE_PART_OF]->(p:Project {{id: '{pid}'}})
                 RETURN count(*) AS cnt"
            ))
            .await?;
        let count = rs
            .rows
            .first()
            .and_then(|r| r.first())
            .map(|v| v.as_i64())
            .unwrap_or(0);
        Ok(count > 0)
    }

    /// Get all file paths in a project.
    pub async fn get_project_files(&self, project_id: Uuid) -> Result<Vec<String>> {
        let pid = escape_cypher(&project_id.to_string());
        let rs = self
            .graph
            .query_rows(format!(
                "MATCH (f:File)-[:FILE_PART_OF]->(p:Project {{id: '{pid}'}})
                 RETURN f.path"
            ))
            .await?;
        Ok(rs
            .rows
            .iter()
            .filter_map(|r| r.first().map(|v| v.as_str().to_string()))
            .collect())
    }

    /// Remove all FILE_PART_OF edges pointing to a project.
    pub async fn unlink_all_files(&self, project_id: Uuid) -> Result<()> {
        let pid = escape_cypher(&project_id.to_string());
        self.graph
            .write(format!(
                "MATCH ()-[r:FILE_PART_OF]->(p:Project {{id: '{pid}'}}) DELETE r"
            ))
            .await?;
        Ok(())
    }

    /// Count distinct files linked to a project that were accessed by an
    /// app active in the given session.
    pub async fn count_session_files(
        &self,
        session_id: &str,
        project_id: Uuid,
    ) -> Result<usize> {
        let pid = escape_cypher(&project_id.to_string());
        let sid = escape_cypher(session_id);
        let rs = self
            .graph
            .query_rows(format!(
                "MATCH (f:File)-[:FILE_PART_OF]->(p:Project {{id: '{pid}'}})
                 MATCH (f)-[:ACCESSED_BY]->(a:App)-[:ACTIVE_IN]->(s:Session {{id: '{sid}'}})
                 RETURN count(DISTINCT f) AS cnt"
            ))
            .await?;
        let count = rs
            .rows
            .first()
            .and_then(|r| r.first())
            .map(|v| v.as_i64())
            .unwrap_or(0);
        Ok(count as usize)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn setup() -> (ProjectStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let graph = crate::graph::spawn(tmp.path().join("graph").to_str().unwrap()).unwrap();
        // Small delay for schema creation.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        (ProjectStore::new(graph), tmp)
    }

    #[tokio::test]
    async fn test_create_and_get_by_id() {
        let (store, _tmp) = setup().await;
        let p = Project::new_inferred("my-app".into(), "/home/user/my-app".into(), 90);
        store.create(&p).await.unwrap();

        let got = store.get_by_id(p.id).await.unwrap();
        assert!(got.is_some());
        let got = got.unwrap();
        assert_eq!(got.name, "my-app");
        assert_eq!(got.root_path, "/home/user/my-app");
        assert!(got.inferred);
        assert_eq!(got.confidence, 90);
        assert!(!got.promoted);
        assert_eq!(got.status, ProjectStatus::Active);
    }

    #[tokio::test]
    async fn test_get_by_root_path() {
        let (store, _tmp) = setup().await;
        let p = Project::new_inferred("app".into(), "/home/user/app".into(), 80);
        store.create(&p).await.unwrap();

        let found = store.get_by_root_path("/home/user/app").await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, p.id);

        let missing = store.get_by_root_path("/home/user/other").await.unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_find_by_path_prefix_nearest_ancestor() {
        let (store, _tmp) = setup().await;

        let parent = Project::new_inferred("monorepo".into(), "/home/user/mono".into(), 90);
        store.create(&parent).await.unwrap();

        let nested = Project::new_inferred(
            "app-a".into(),
            "/home/user/mono/packages/app-a".into(),
            100,
        );
        store.create(&nested).await.unwrap();

        // File inside nested -> nearest ancestor = nested
        let found = store
            .find_by_path_prefix("/home/user/mono/packages/app-a/src/main.rs")
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "app-a");

        // File in parent but outside nested -> nearest = parent
        let found = store
            .find_by_path_prefix("/home/user/mono/docs/readme.md")
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "monorepo");

        // File outside everything -> None
        let found = store
            .find_by_path_prefix("/home/user/downloads/file.txt")
            .await
            .unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_list_active() {
        let (store, _tmp) = setup().await;

        let a = Project::new_inferred("active".into(), "/a".into(), 90);
        let b = Project::new_inferred("to-archive".into(), "/b".into(), 90);
        store.create(&a).await.unwrap();
        store.create(&b).await.unwrap();
        store.archive(b.id).await.unwrap();

        let active = store.list_active().await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].name, "active");
    }

    #[tokio::test]
    async fn test_list_promoted() {
        let (store, _tmp) = setup().await;

        let explicit = Project::new_explicit(Uuid::now_v7(), "explicit".into(), "/a".into());
        let inferred = Project::new_inferred("inferred".into(), "/b".into(), 80);
        store.create(&explicit).await.unwrap();
        store.create(&inferred).await.unwrap();

        let promoted = store.list_promoted().await.unwrap();
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].name, "explicit");
    }

    #[tokio::test]
    async fn test_promote() {
        let (store, _tmp) = setup().await;

        let p = Project::new_inferred("test".into(), "/a".into(), 90);
        store.create(&p).await.unwrap();
        assert!(!p.promoted);

        store.promote(p.id).await.unwrap();

        let got = store.get_by_id(p.id).await.unwrap().unwrap();
        assert!(got.promoted);
    }

    #[tokio::test]
    async fn test_archive() {
        let (store, _tmp) = setup().await;

        let p = Project::new_inferred("test".into(), "/a".into(), 90);
        store.create(&p).await.unwrap();
        store.archive(p.id).await.unwrap();

        let got = store.get_by_id(p.id).await.unwrap().unwrap();
        assert_eq!(got.status, ProjectStatus::Archived);
        assert!(got.archived_at.is_some());
    }

    #[tokio::test]
    async fn test_touch() {
        let (store, _tmp) = setup().await;

        let p = Project::new_inferred("test".into(), "/a".into(), 90);
        store.create(&p).await.unwrap();

        store.touch(p.id).await.unwrap();

        let got = store.get_by_id(p.id).await.unwrap().unwrap();
        assert!(got.last_accessed.is_some());
    }

    #[tokio::test]
    async fn test_delete() {
        let (store, _tmp) = setup().await;

        let p = Project::new_inferred("test".into(), "/a".into(), 90);
        store.create(&p).await.unwrap();
        store.delete(p.id).await.unwrap();

        let got = store.get_by_id(p.id).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn test_unique_root_path() {
        let (store, _tmp) = setup().await;

        let p1 = Project::new_inferred("first".into(), "/same".into(), 90);
        let p2 = Project::new_inferred("second".into(), "/same".into(), 90);
        store.create(&p1).await.unwrap();

        let result = store.create(&p2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update() {
        let (store, _tmp) = setup().await;

        let mut p = Project::new_inferred("old-name".into(), "/a".into(), 80);
        store.create(&p).await.unwrap();

        p.name = "new-name".into();
        p.description = "A description".into();
        p.accent_color = "#6366f1".into();
        p.confidence = 100;
        store.update(&p).await.unwrap();

        let got = store.get_by_id(p.id).await.unwrap().unwrap();
        assert_eq!(got.name, "new-name");
        assert_eq!(got.description, "A description");
        assert_eq!(got.accent_color, "#6366f1");
        assert_eq!(got.confidence, 100);
    }

    #[tokio::test]
    async fn test_link_file_and_check() {
        let (store, _tmp) = setup().await;

        let p = Project::new_inferred("test".into(), "/a".into(), 90);
        store.create(&p).await.unwrap();

        // Create a File node.
        let file_path = "/a/src/main.rs";
        store
            .graph
            .write(format!(
                "CREATE (f:File {{id: '{file_path}', path: '{file_path}', \
                 app_id: 'test', last_accessed: 0}})"
            ))
            .await
            .unwrap();

        // Link file to project.
        store.link_file(file_path, p.id).await.unwrap();
        assert!(store.is_file_linked(file_path, p.id).await.unwrap());

        // Get project files.
        let files = store.get_project_files(p.id).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], file_path);
    }

    #[tokio::test]
    async fn test_unlink_all_files() {
        let (store, _tmp) = setup().await;

        let p = Project::new_inferred("test".into(), "/a".into(), 90);
        store.create(&p).await.unwrap();

        // Create two file nodes.
        for path in ["/a/one.rs", "/a/two.rs"] {
            store
                .graph
                .write(format!(
                    "CREATE (f:File {{id: '{path}', path: '{path}', \
                     app_id: 'test', last_accessed: 0}})"
                ))
                .await
                .unwrap();
            store.link_file(path, p.id).await.unwrap();
        }

        assert_eq!(store.get_project_files(p.id).await.unwrap().len(), 2);

        store.unlink_all_files(p.id).await.unwrap();
        assert_eq!(store.get_project_files(p.id).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_explicit_project_is_promoted() {
        let (store, _tmp) = setup().await;

        let p = Project::new_explicit(Uuid::now_v7(), "explicit".into(), "/a".into());
        assert!(p.promoted);
        assert!(!p.inferred);
        assert_eq!(p.confidence, 100);

        store.create(&p).await.unwrap();
        let got = store.get_by_id(p.id).await.unwrap().unwrap();
        assert!(got.promoted);
        assert!(!got.inferred);
    }
}
