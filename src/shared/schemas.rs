/// Built-in shared entity schemas: Person, Organization, Event, Location, Tag.

use std::collections::HashMap;

use crate::schema::{EntityDefinition, FieldDefinition, FieldType, LifecycleConfig};

/// All shared entity type names.
pub fn shared_entity_types() -> Vec<&'static str> {
    vec![
        "shared.Person",
        "shared.Organization",
        "shared.Event",
        "shared.Location",
        "shared.Tag",
    ]
}

/// Get all shared entity definitions.
pub fn shared_schemas() -> HashMap<String, EntityDefinition> {
    let mut m = HashMap::new();
    m.insert("shared.Person".into(), person_schema());
    m.insert("shared.Organization".into(), organization_schema());
    m.insert("shared.Event".into(), event_schema());
    m.insert("shared.Location".into(), location_schema());
    m.insert("shared.Tag".into(), tag_schema());
    m
}

fn field(ft: FieldType) -> FieldDefinition {
    FieldDefinition { field_type: ft, ..Default::default() }
}

fn person_schema() -> EntityDefinition {
    let mut fields = HashMap::new();
    fields.insert("name".into(), FieldDefinition {
        field_type: FieldType::String, required: true, indexed: true, ..Default::default()
    });
    fields.insert("email".into(), FieldDefinition {
        field_type: FieldType::Email, indexed: true, unique: true, sensitive: true, ..Default::default()
    });
    fields.insert("phone".into(), FieldDefinition {
        field_type: FieldType::String, sensitive: true, ..Default::default()
    });
    fields.insert("avatar_url".into(), field(FieldType::Url));
    fields.insert("notes".into(), field(FieldType::Text));
    fields.insert("normalized_name".into(), FieldDefinition {
        field_type: FieldType::String, indexed: true, ..Default::default()
    });
    EntityDefinition {
        fields,
        lifecycle: LifecycleConfig { trash_retention_days: 90, history_enabled: true },
        ..Default::default()
    }
}

fn organization_schema() -> EntityDefinition {
    let mut fields = HashMap::new();
    fields.insert("name".into(), FieldDefinition {
        field_type: FieldType::String, required: true, indexed: true, ..Default::default()
    });
    fields.insert("domain".into(), FieldDefinition {
        field_type: FieldType::String, indexed: true, unique: true, ..Default::default()
    });
    fields.insert("website".into(), field(FieldType::Url));
    fields.insert("industry".into(), FieldDefinition {
        field_type: FieldType::String, indexed: true, ..Default::default()
    });
    fields.insert("normalized_name".into(), FieldDefinition {
        field_type: FieldType::String, indexed: true, ..Default::default()
    });
    EntityDefinition {
        fields,
        lifecycle: LifecycleConfig { trash_retention_days: 90, history_enabled: true },
        ..Default::default()
    }
}

fn event_schema() -> EntityDefinition {
    let mut fields = HashMap::new();
    fields.insert("title".into(), FieldDefinition {
        field_type: FieldType::String, required: true, indexed: true, ..Default::default()
    });
    fields.insert("description".into(), field(FieldType::Text));
    fields.insert("start_time".into(), FieldDefinition {
        field_type: FieldType::Datetime, required: true, indexed: true, ..Default::default()
    });
    fields.insert("end_time".into(), field(FieldType::Datetime));
    fields.insert("all_day".into(), FieldDefinition {
        field_type: FieldType::Bool,
        default: Some(toml::Value::Boolean(false)),
        ..Default::default()
    });
    fields.insert("recurrence".into(), field(FieldType::String));
    EntityDefinition { fields, ..Default::default() }
}

fn location_schema() -> EntityDefinition {
    let mut fields = HashMap::new();
    fields.insert("name".into(), FieldDefinition {
        field_type: FieldType::String, required: true, indexed: true, ..Default::default()
    });
    fields.insert("address".into(), field(FieldType::String));
    fields.insert("latitude".into(), field(FieldType::Float));
    fields.insert("longitude".into(), field(FieldType::Float));
    fields.insert("place_id".into(), FieldDefinition {
        field_type: FieldType::String, indexed: true, unique: true, ..Default::default()
    });
    EntityDefinition { fields, ..Default::default() }
}

fn tag_schema() -> EntityDefinition {
    let mut fields = HashMap::new();
    fields.insert("name".into(), FieldDefinition {
        field_type: FieldType::String, required: true, indexed: true, unique: true, ..Default::default()
    });
    fields.insert("color".into(), field(FieldType::Color));
    fields.insert("icon".into(), field(FieldType::String));
    EntityDefinition {
        fields,
        lifecycle: LifecycleConfig { trash_retention_days: 7, history_enabled: false },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_entity_types() {
        let types = shared_entity_types();
        assert_eq!(types.len(), 5);
        assert!(types.contains(&"shared.Person"));
        assert!(types.contains(&"shared.Tag"));
    }

    #[test]
    fn test_shared_schemas_complete() {
        let schemas = shared_schemas();
        assert_eq!(schemas.len(), 5);
        for t in shared_entity_types() {
            assert!(schemas.contains_key(t), "missing schema for {t}");
        }
    }

    #[test]
    fn test_person_sensitive_fields() {
        let schemas = shared_schemas();
        let person = &schemas["shared.Person"];
        assert!(person.fields["email"].sensitive);
        assert!(person.fields["phone"].sensitive);
        assert!(!person.fields["name"].sensitive);
    }

    #[test]
    fn test_person_required_fields() {
        let schemas = shared_schemas();
        let person = &schemas["shared.Person"];
        assert!(person.fields["name"].required);
        assert!(!person.fields["email"].required);
    }

    #[test]
    fn test_tag_short_retention() {
        let schemas = shared_schemas();
        assert_eq!(schemas["shared.Tag"].lifecycle.trash_retention_days, 7);
        assert_eq!(schemas["shared.Person"].lifecycle.trash_retention_days, 90);
    }

    #[test]
    fn test_location_unique_place_id() {
        let schemas = shared_schemas();
        assert!(schemas["shared.Location"].fields["place_id"].unique);
    }

    #[test]
    fn test_event_required_fields() {
        let schemas = shared_schemas();
        let event = &schemas["shared.Event"];
        assert!(event.fields["title"].required);
        assert!(event.fields["start_time"].required);
        assert!(!event.fields["end_time"].required);
    }
}
