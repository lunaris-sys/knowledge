/// Entity creation with token scope enforcement and reserved field injection.

use std::collections::HashMap;

use chrono::Utc;
use thiserror::Error;
use uuid::Uuid;

use crate::schema::SchemaRegistry;
use crate::token::{CapabilityToken, InstanceScope};

use super::validation::{FieldValidator, WriteValidationError};

/// Errors from create operations.
#[derive(Debug, Error)]
pub enum CreateError {
    #[error("validation: {0}")]
    Validation(#[from] WriteValidationError),
    #[error("permission denied: cannot write to {0}")]
    PermissionDenied(String),
    #[error("namespace violation: {app_id} cannot create {entity_type}")]
    NamespaceViolation { app_id: String, entity_type: String },
    /// The caller's token does not grant this exact relation. The relation scope
    /// is the authorization: a relation is permitted only if the token carries a
    /// matching `(from, to, relation_type)` scope (resolved from the caller's
    /// permission profile), so an undeclared relation is refused fail-closed.
    #[error("permission denied: cannot create relation {from}-[{relation_type}]->{to}")]
    RelationDenied {
        from: String,
        to: String,
        relation_type: String,
    },
    /// A relation endpoint names an entity type the registry does not know.
    #[error("unknown entity type: {0}")]
    UnknownEntityType(String),
    /// A relation endpoint has an empty id; a relation needs two concrete nodes.
    #[error("a relation endpoint id is empty")]
    EmptyEndpointId,
    /// A relation whose endpoints are both outside the caller's namespace (e.g.
    /// linking two system nodes the caller does not own) needs a privileged
    /// `InstanceScope::All` token. A type-level relation scope alone must not let
    /// an unprivileged app rewrite global facts between nodes it does not own.
    #[error("relation {from}-[{relation_type}]->{to} has no caller-owned anchor and the token is not privileged (InstanceScope::All)")]
    UnanchoredRelationRequiresPrivilege {
        from: String,
        to: String,
        relation_type: String,
    },
    /// The (from, to, relation_type) triple is not a declared relation, so a
    /// scope (even a matching one) must not create an undeclared edge.
    #[error("undeclared relation {from}-[{relation_type}]->{to}")]
    UndeclaredRelation {
        from: String,
        to: String,
        relation_type: String,
    },
}

/// The built-in graph relations (the Ladybug `REL TABLE`s in `graph.rs`), in the
/// registry's namespaced form. These are the only system-level relations that
/// exist, so a relation between system types is valid only if it is one of
/// these. This list MUST stay in sync with the `CREATE REL TABLE` statements in
/// `graph.rs`; single-sourcing the two (and the registry's system node list)
/// from one definition is the robust fix against drift, tracked as a follow-up.
/// App-defined schema relations (`[relations.X]`) are a separate follow-up: the
/// only current write consumer (the agent) writes built-in system relations.
const BUILTIN_RELATIONS: &[(&str, &str, &str)] = &[
    ("system.File", "system.App", "ACCESSED_BY"),
    ("system.App", "system.Session", "ACTIVE_IN"),
    ("system.Event", "system.App", "EMITTED_BY"),
    ("system.UserAction", "system.Event", "DERIVED_FROM"),
    ("system.File", "system.Project", "FILE_PART_OF"),
    ("system.Directory", "system.Project", "DIR_PART_OF"),
    ("system.Summary", "system.App", "SUMMARIZES"),
];

/// Result of a successful create operation (before DB write).
pub struct CreateResult {
    pub id: Uuid,
    pub entity_type: String,
    pub data: HashMap<String, serde_json::Value>,
}

/// Executes a create operation: validates, checks scopes, injects reserved fields.
pub fn create_entity(
    registry: &SchemaRegistry,
    entity_type: &str,
    data: HashMap<String, serde_json::Value>,
    token: &CapabilityToken,
) -> Result<CreateResult, CreateError> {
    // 1. Token write scope check.
    if !token.can_write(entity_type) {
        return Err(CreateError::PermissionDenied(entity_type.into()));
    }

    // 2. Namespace check: app can only create in its own namespace.
    check_namespace(entity_type, &token.app_id)?;

    // 3. Field validation.
    let validator = FieldValidator::new(registry);
    validator.validate_create(entity_type, &data)?;

    // 4. Build entity with reserved fields.
    let id = Uuid::now_v7();
    let now = Utc::now().to_rfc3339();

    let mut entity = data;
    entity.insert("id".into(), serde_json::json!(id.to_string()));
    entity.insert("_version".into(), serde_json::json!(1));
    entity.insert("_owner".into(), serde_json::json!(token.app_id));
    entity.insert("_created_at".into(), serde_json::json!(now));
    entity.insert("_modified_at".into(), serde_json::json!(now));
    entity.insert("_deleted".into(), serde_json::json!(false));

    // 5. Apply defaults for missing optional fields.
    if let Some(entity_def) = registry.get_entity(entity_type) {
        for (field_name, field_def) in &entity_def.fields {
            if !entity.contains_key(field_name) {
                if let Some(default) = &field_def.default {
                    entity.insert(field_name.clone(), toml_to_json(default));
                }
            }
        }
    }

    Ok(CreateResult {
        id,
        entity_type: entity_type.into(),
        data: entity,
    })
}

/// A validated relation create, before it is persisted. Holds the registry
/// (namespaced) entity types and the concrete endpoint ids; the caller maps the
/// namespaced types to their graph table names when it builds the persistence
/// query.
#[derive(Debug)]
pub struct RelationResult {
    pub from_type: String,
    pub from_id: String,
    pub to_type: String,
    pub to_id: String,
    pub relation_type: String,
}

/// Authorise and validate a relation create between two existing nodes.
///
/// The authorisation is layered, all fail-closed:
/// 1. **Relation scope** — the token must carry a matching
///    `(from_type, to_type, relation_type)` scope (from its permission profile),
///    so only a relation the caller was granted is permitted.
/// 2. **Anchor / privilege** — a type-level scope is not by itself enough to
///    rewrite global facts: if **neither** endpoint is in the caller's namespace
///    (e.g. linking two system nodes it does not own), the token must be
///    privileged (`InstanceScope::All`). An app can freely relate its own
///    entities; relating nodes it does not own needs the privileged grant.
/// 3. **Declared relation** — the triple must be a real relation (a built-in
///    graph relation here), so a scope cannot create an undeclared edge.
/// 4. **Known types / concrete ids** — endpoint types must exist and ids be
///    non-empty.
///
/// This validates and *authorises*; it does not prove the endpoint *instances*
/// exist. The persistence layer carries that obligation and MUST be a checked
/// operation: `MATCH` both endpoints by type+id, enforce ownership/visibility on
/// the matched rows, `MERGE` the edge, and return not-found if a `MATCH` bound
/// nothing — never report success for a no-op against absent nodes. The
/// returned [`RelationResult`] is exactly what to persist under that contract.
pub fn create_relation(
    registry: &SchemaRegistry,
    from_type: &str,
    from_id: &str,
    to_type: &str,
    to_id: &str,
    relation_type: &str,
    token: &CapabilityToken,
) -> Result<RelationResult, CreateError> {
    // 1. Relation scope check: only a granted relation may be written.
    if !token.can_create_relation(from_type, to_type, relation_type) {
        return Err(CreateError::RelationDenied {
            from: from_type.into(),
            to: to_type.into(),
            relation_type: relation_type.into(),
        });
    }

    // 2. Anchor / privilege: relating nodes the caller does not own (neither
    //    endpoint in its namespace) needs a privileged InstanceScope::All token,
    //    so a type-level scope cannot rewrite arbitrary global facts.
    let owns = |t: &str| t.starts_with(&format!("{}.", token.app_id));
    let has_anchor = owns(from_type) || owns(to_type);
    if !has_anchor && token.instance_scope != InstanceScope::All {
        return Err(CreateError::UnanchoredRelationRequiresPrivilege {
            from: from_type.into(),
            to: to_type.into(),
            relation_type: relation_type.into(),
        });
    }

    // 3. The triple must be a declared relation, so a (mis)granted scope cannot
    //    create an undeclared edge.
    let declared = BUILTIN_RELATIONS
        .iter()
        .any(|&(f, t, r)| f == from_type && t == to_type && r == relation_type);
    if !declared {
        return Err(CreateError::UndeclaredRelation {
            from: from_type.into(),
            to: to_type.into(),
            relation_type: relation_type.into(),
        });
    }

    // 4. Both endpoint types must be real entity types the registry knows.
    if !registry.entity_exists(from_type) {
        return Err(CreateError::UnknownEntityType(from_type.into()));
    }
    if !registry.entity_exists(to_type) {
        return Err(CreateError::UnknownEntityType(to_type.into()));
    }

    // 5. Both endpoints must be concrete nodes.
    if from_id.is_empty() || to_id.is_empty() {
        return Err(CreateError::EmptyEndpointId);
    }

    Ok(RelationResult {
        from_type: from_type.into(),
        from_id: from_id.into(),
        to_type: to_type.into(),
        to_id: to_id.into(),
        relation_type: relation_type.into(),
    })
}

fn check_namespace(entity_type: &str, app_id: &str) -> Result<(), CreateError> {
    if app_id == "system" && entity_type.starts_with("system.") {
        return Ok(());
    }
    let prefix = format!("{app_id}.");
    if !entity_type.starts_with(&prefix) {
        return Err(CreateError::NamespaceViolation {
            app_id: app_id.into(),
            entity_type: entity_type.into(),
        });
    }
    Ok(())
}

fn toml_to_json(v: &toml::Value) -> serde_json::Value {
    match v {
        toml::Value::String(s) => serde_json::json!(s),
        toml::Value::Integer(i) => serde_json::json!(i),
        toml::Value::Float(f) => serde_json::json!(f),
        toml::Value::Boolean(b) => serde_json::json!(b),
        toml::Value::Array(a) => {
            serde_json::Value::Array(a.iter().map(toml_to_json).collect())
        }
        _ => serde_json::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaRegistry;
    use crate::token::{CapabilityToken, EntityScope, InstanceScope, RelationScope};

    /// A token that grants exactly the File -> Project FILE_PART_OF relation.
    fn relation_token() -> CapabilityToken {
        CapabilityToken::new(
            "org.lunaris.agent".into(),
            1234,
            vec![],
            vec![],
            vec![RelationScope {
                from: "system.File".into(),
                to: "system.Project".into(),
                relation_type: "FILE_PART_OF".into(),
            }],
            InstanceScope::All,
        )
    }

    fn setup() -> (SchemaRegistry, CapabilityToken) {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(
            r#"
[meta]
namespace = "com.test"

[entities.Note]
[entities.Note.fields.title]
type = "string"
required = true

[entities.Note.fields.body]
type = "text"

[entities.Note.fields.score]
type = "float"
default = 1.0
"#,
        )
        .unwrap();

        let token = CapabilityToken::new(
            "com.test".into(),
            1234,
            vec![],
            vec![EntityScope {
                entity_type: "com.test.*".into(),
                fields: None,
                exclude_fields: vec![],
            }],
            vec![],
            InstanceScope::Own,
        );

        (reg, token)
    }

    #[test]
    fn test_create_success() {
        let (reg, token) = setup();
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));

        let result = create_entity(&reg, "com.test.Note", data, &token).unwrap();
        assert_eq!(result.entity_type, "com.test.Note");
        assert!(result.data.contains_key("id"));
        assert_eq!(result.data["_version"], 1);
        assert_eq!(result.data["_owner"], "com.test");
        assert_eq!(result.data["_deleted"], false);
        assert!(result.data.contains_key("_created_at"));
        assert!(result.data.contains_key("_modified_at"));
    }

    #[test]
    fn test_create_defaults_applied() {
        let (reg, token) = setup();
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));

        let result = create_entity(&reg, "com.test.Note", data, &token).unwrap();
        assert_eq!(result.data["score"], 1.0);
    }

    #[test]
    fn test_create_permission_denied() {
        let (reg, _) = setup();
        let token = CapabilityToken::new("com.test".into(), 1, vec![], vec![], vec![], InstanceScope::Own);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));

        assert!(matches!(
            create_entity(&reg, "com.test.Note", data, &token),
            Err(CreateError::PermissionDenied(_))
        ));
    }

    #[test]
    fn test_create_namespace_violation() {
        let (reg, _) = setup();
        // Token with write scope for com.other.* but app_id is com.test.
        // This tests that even with write scope, namespace must match app_id.
        let token = CapabilityToken::new(
            "com.test".into(),
            1234,
            vec![],
            vec![EntityScope {
                entity_type: "com.other.*".into(),
                fields: None,
                exclude_fields: vec![],
            }],
            vec![],
            InstanceScope::Own,
        );
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));

        assert!(matches!(
            create_entity(&reg, "com.other.Note", data, &token),
            Err(CreateError::NamespaceViolation { .. })
        ));
    }

    #[test]
    fn test_create_validation_error() {
        let (reg, token) = setup();
        let data = HashMap::new(); // missing required title

        assert!(matches!(
            create_entity(&reg, "com.test.Note", data, &token),
            Err(CreateError::Validation(_))
        ));
    }

    #[test]
    fn test_create_relation_success() {
        let (reg, _) = setup();
        let token = relation_token();
        let result = create_relation(
            &reg,
            "system.File",
            "f1",
            "system.Project",
            "p1",
            "FILE_PART_OF",
            &token,
        )
        .unwrap();
        assert_eq!(result.from_type, "system.File");
        assert_eq!(result.to_type, "system.Project");
        assert_eq!(result.from_id, "f1");
        assert_eq!(result.to_id, "p1");
        assert_eq!(result.relation_type, "FILE_PART_OF");
    }

    #[test]
    fn test_create_relation_denied_without_scope() {
        let (reg, _) = setup();
        // A token with no relation scope cannot create the relation.
        let token =
            CapabilityToken::new("org.lunaris.agent".into(), 1, vec![], vec![], vec![], InstanceScope::All);
        assert!(matches!(
            create_relation(&reg, "system.File", "f1", "system.Project", "p1", "FILE_PART_OF", &token),
            Err(CreateError::RelationDenied { .. })
        ));
    }

    #[test]
    fn test_create_relation_denied_for_a_different_relation() {
        let (reg, _) = setup();
        // The scope is exact: a different relation type (or endpoints) is refused.
        let token = relation_token();
        assert!(matches!(
            create_relation(&reg, "system.File", "f1", "system.Project", "p1", "OWNS", &token),
            Err(CreateError::RelationDenied { .. })
        ));
        assert!(matches!(
            create_relation(&reg, "system.Project", "p1", "system.File", "f1", "FILE_PART_OF", &token),
            Err(CreateError::RelationDenied { .. })
        ));
    }

    #[test]
    fn test_all_builtin_relations_are_creatable() {
        // Every declared built-in relation must be creatable with a matching
        // privileged scope: its endpoint types are registered and the triple is
        // in the allowlist. This catches drift between BUILTIN_RELATIONS, the
        // registry's system types, and graph.rs's REL TABLEs.
        let (reg, _) = setup();
        for &(from, to, rel) in BUILTIN_RELATIONS {
            let token = CapabilityToken::new(
                "org.lunaris.agent".into(),
                1,
                vec![],
                vec![],
                vec![RelationScope {
                    from: from.into(),
                    to: to.into(),
                    relation_type: rel.into(),
                }],
                InstanceScope::All,
            );
            let result = create_relation(&reg, from, "a", to, "b", rel, &token);
            assert!(
                result.is_ok(),
                "built-in relation {from}-[{rel}]->{to} should be creatable, got {result:?}"
            );
        }
    }

    #[test]
    fn test_create_relation_undeclared_relation_is_refused() {
        let (reg, _) = setup();
        // The token grants an exact scope, but the triple is not a real
        // (built-in) relation, so it must not create an undeclared edge.
        let token = CapabilityToken::new(
            "org.lunaris.agent".into(),
            1,
            vec![],
            vec![],
            vec![RelationScope {
                from: "system.File".into(),
                to: "system.Project".into(),
                relation_type: "BOGUS_REL".into(),
            }],
            InstanceScope::All,
        );
        assert!(matches!(
            create_relation(&reg, "system.File", "f1", "system.Project", "p1", "BOGUS_REL", &token),
            Err(CreateError::UndeclaredRelation { .. })
        ));
    }

    #[test]
    fn test_create_relation_unanchored_requires_privilege() {
        let (reg, _) = setup();
        // A FILE_PART_OF scope between two system nodes the caller does not own,
        // but only InstanceScope::Own: refused, since neither endpoint anchors
        // the relation in the caller's namespace.
        let token = CapabilityToken::new(
            "org.lunaris.agent".into(),
            1,
            vec![],
            vec![],
            vec![RelationScope {
                from: "system.File".into(),
                to: "system.Project".into(),
                relation_type: "FILE_PART_OF".into(),
            }],
            InstanceScope::Own,
        );
        assert!(matches!(
            create_relation(&reg, "system.File", "f1", "system.Project", "p1", "FILE_PART_OF", &token),
            Err(CreateError::UnanchoredRelationRequiresPrivilege { .. })
        ));
    }

    #[test]
    fn test_create_relation_empty_endpoint_id() {
        let (reg, _) = setup();
        let token = relation_token();
        assert!(matches!(
            create_relation(&reg, "system.File", "", "system.Project", "p1", "FILE_PART_OF", &token),
            Err(CreateError::EmptyEndpointId)
        ));
    }
}
