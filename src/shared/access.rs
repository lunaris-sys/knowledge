/// Access control for shared entities.

use crate::token::CapabilityToken;

/// Apps allowed to create/write shared entities.
const SHARED_WRITERS: &[&str] = &[
    "org.lunaris.contacts",
    "org.lunaris.calendar",
    "org.lunaris.places",
    "system",
];

/// Whether an app can write (create/update/delete) shared entities.
pub fn can_write_shared(app_id: &str) -> bool {
    SHARED_WRITERS.contains(&app_id) || app_id.starts_with("org.lunaris.")
}

/// Whether a token allows reading a shared entity type.
pub fn can_read_shared(token: &CapabilityToken, entity_type: &str) -> bool {
    token.can_read(entity_type)
}

/// Whether a token allows reading sensitive fields of a shared entity type.
///
/// First-party shared writers always have sensitive access.
pub fn can_read_sensitive(token: &CapabilityToken, entity_type: &str) -> bool {
    if can_write_shared(&token.app_id) {
        return true;
    }
    // Check if token explicitly includes this type in read scopes
    // (full read_sensitive support requires Phase 3.2 permission profile integration)
    token.read_scopes.iter().any(|s| {
        s.entity_type == entity_type || s.entity_type == "shared.*"
    })
}

/// Null out sensitive fields the token is not allowed to see.
pub fn filter_sensitive_fields(
    data: &mut serde_json::Map<String, serde_json::Value>,
    entity_type: &str,
    token: &CapabilityToken,
) {
    if can_read_sensitive(token, entity_type) {
        return;
    }
    for field in sensitive_fields_for_type(entity_type) {
        if data.contains_key(*field) {
            data.insert(field.to_string(), serde_json::Value::Null);
        }
    }
}

/// Sensitive fields per shared entity type.
fn sensitive_fields_for_type(entity_type: &str) -> &'static [&'static str] {
    match entity_type {
        "shared.Person" => &["email", "phone"],
        _ => &[],
    }
}

/// Whether a token allows creating a relation to a shared entity.
pub fn can_relate_to_shared(
    token: &CapabilityToken,
    shared_type: &str,
    relation_type: &str,
) -> bool {
    token.relation_scopes.iter().any(|s| {
        s.to == shared_type && s.relation_type == relation_type
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{CapabilityToken, EntityScope, InstanceScope, RelationScope};

    fn read_token(scopes: Vec<&str>) -> CapabilityToken {
        CapabilityToken::new(
            "com.test".into(),
            1,
            scopes.into_iter().map(|s| EntityScope {
                entity_type: s.into(), fields: None, exclude_fields: vec![],
            }).collect(),
            vec![],
            vec![],
            InstanceScope::Own,
        )
    }

    #[test]
    fn test_can_write_shared_first_party() {
        assert!(can_write_shared("org.lunaris.contacts"));
        assert!(can_write_shared("org.lunaris.calendar"));
        assert!(can_write_shared("org.lunaris.places"));
        assert!(can_write_shared("system"));
    }

    #[test]
    fn test_can_write_shared_third_party_denied() {
        assert!(!can_write_shared("com.anki"));
        assert!(!can_write_shared("org.zotero"));
    }

    #[test]
    fn test_can_read_shared() {
        let token = read_token(vec!["shared.Person"]);
        assert!(can_read_shared(&token, "shared.Person"));
        assert!(!can_read_shared(&token, "shared.Organization"));
    }

    #[test]
    fn test_can_read_shared_wildcard() {
        let token = read_token(vec!["shared.*"]);
        assert!(can_read_shared(&token, "shared.Person"));
        assert!(can_read_shared(&token, "shared.Tag"));
    }

    #[test]
    fn test_can_read_sensitive_first_party() {
        let mut token = read_token(vec!["shared.Person"]);
        token.app_id = "org.lunaris.contacts".into();
        assert!(can_read_sensitive(&token, "shared.Person"));
    }

    #[test]
    fn test_can_read_sensitive_third_party_denied() {
        let token = read_token(vec![]); // no shared scopes
        assert!(!can_read_sensitive(&token, "shared.Person"));
    }

    #[test]
    fn test_filter_sensitive_fields() {
        // Token with NO shared scopes (third party without sensitive access).
        let token = read_token(vec!["com.test.*"]);
        let mut data = serde_json::Map::new();
        data.insert("name".into(), serde_json::json!("Alice"));
        data.insert("email".into(), serde_json::json!("alice@test.com"));
        data.insert("phone".into(), serde_json::json!("+1234"));

        filter_sensitive_fields(&mut data, "shared.Person", &token);

        assert_eq!(data["name"], "Alice");
        assert!(data["email"].is_null()); // filtered
        assert!(data["phone"].is_null()); // filtered
    }

    #[test]
    fn test_filter_sensitive_first_party_sees_all() {
        let mut token = read_token(vec!["shared.Person"]);
        token.app_id = "org.lunaris.contacts".into();
        let mut data = serde_json::Map::new();
        data.insert("email".into(), serde_json::json!("alice@test.com"));

        filter_sensitive_fields(&mut data, "shared.Person", &token);

        assert_eq!(data["email"], "alice@test.com"); // not filtered
    }

    #[test]
    fn test_can_relate_to_shared() {
        let token = CapabilityToken::new(
            "com.test".into(), 1, vec![], vec![],
            vec![RelationScope {
                from: "com.test.Note".into(),
                to: "shared.Person".into(),
                relation_type: "MENTIONS".into(),
            }],
            InstanceScope::Own,
        );
        assert!(can_relate_to_shared(&token, "shared.Person", "MENTIONS"));
        assert!(!can_relate_to_shared(&token, "shared.Person", "CREATED"));
        assert!(!can_relate_to_shared(&token, "shared.Tag", "MENTIONS"));
    }
}
