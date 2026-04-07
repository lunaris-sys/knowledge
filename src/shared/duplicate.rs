/// Duplicate detection for shared entities.

use std::collections::HashSet;

use serde_json::Value;

/// A potential duplicate match.
#[derive(Debug, Clone)]
pub struct DuplicateCandidate {
    pub existing_id: String,
    pub match_score: f64,
    pub match_fields: Vec<String>,
}

/// Per-type duplicate detection config.
#[derive(Debug, Clone)]
pub struct DuplicateConfig {
    pub unique_fields: Vec<String>,
    pub fuzzy_fields: Vec<(String, f64)>,
    pub min_score: f64,
}

impl DuplicateConfig {
    /// Get the duplicate config for a shared entity type.
    pub fn for_type(entity_type: &str) -> Self {
        match entity_type {
            "shared.Person" => Self {
                unique_fields: vec!["email".into()],
                fuzzy_fields: vec![("normalized_name".into(), 0.85)],
                min_score: 0.8,
            },
            "shared.Organization" => Self {
                unique_fields: vec!["domain".into()],
                fuzzy_fields: vec![("normalized_name".into(), 0.9)],
                min_score: 0.85,
            },
            "shared.Location" => Self {
                unique_fields: vec!["place_id".into()],
                fuzzy_fields: vec![("name".into(), 0.9), ("address".into(), 0.85)],
                min_score: 0.8,
            },
            "shared.Tag" => Self {
                unique_fields: vec!["name".into()],
                fuzzy_fields: vec![],
                min_score: 1.0,
            },
            _ => Self {
                unique_fields: vec![],
                fuzzy_fields: vec![],
                min_score: 1.0,
            },
        }
    }
}

/// Checks new data against one existing entity.
pub fn check_duplicate(
    config: &DuplicateConfig,
    new_data: &serde_json::Map<String, Value>,
    existing_id: &str,
    existing_data: &serde_json::Map<String, Value>,
) -> Option<DuplicateCandidate> {
    let mut score_sum = 0.0;
    let mut field_count = 0;
    let mut match_fields = Vec::new();

    for field in &config.unique_fields {
        if let (Some(nv), Some(ev)) = (new_data.get(field), existing_data.get(field)) {
            field_count += 1;
            if nv == ev && !nv.is_null() {
                score_sum += 1.0;
                match_fields.push(field.clone());
            }
        }
    }

    for (field, min_sim) in &config.fuzzy_fields {
        if let (Some(Value::String(nv)), Some(Value::String(ev))) =
            (new_data.get(field), existing_data.get(field))
        {
            field_count += 1;
            let sim = string_similarity(nv, ev);
            if sim >= *min_sim {
                score_sum += sim;
                match_fields.push(field.clone());
            }
        }
    }

    let normalized = if field_count > 0 {
        score_sum / field_count as f64
    } else {
        0.0
    };

    if normalized >= config.min_score && !match_fields.is_empty() {
        Some(DuplicateCandidate {
            existing_id: existing_id.into(),
            match_score: normalized,
            match_fields,
        })
    } else {
        None
    }
}

/// Normalize a name for comparison (lowercase, collapse whitespace).
pub fn normalize_name(name: &str) -> String {
    name.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Simple Jaccard similarity on character sets.
fn string_similarity(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }
    let al = a.to_lowercase();
    let bl = b.to_lowercase();
    if al == bl {
        return 0.95;
    }
    let ac: HashSet<char> = al.chars().collect();
    let bc: HashSet<char> = bl.chars().collect();
    let inter = ac.intersection(&bc).count();
    let union = ac.union(&bc).count();
    if union == 0 { 0.0 } else { inter as f64 / union as f64 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(pairs: &[(&str, Value)]) -> serde_json::Map<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn test_exact_match_email() {
        let cfg = DuplicateConfig::for_type("shared.Person");
        let new_data = map(&[("email", json!("alice@test.com"))]);
        let existing = map(&[("email", json!("alice@test.com"))]);
        let dup = check_duplicate(&cfg, &new_data, "e1", &existing);
        assert!(dup.is_some());
        assert!(dup.unwrap().match_fields.contains(&"email".to_string()));
    }

    #[test]
    fn test_no_match_different_email() {
        let cfg = DuplicateConfig::for_type("shared.Person");
        let new_data = map(&[("email", json!("alice@test.com"))]);
        let existing = map(&[("email", json!("bob@test.com"))]);
        let dup = check_duplicate(&cfg, &new_data, "e1", &existing);
        assert!(dup.is_none());
    }

    #[test]
    fn test_fuzzy_name_match() {
        let cfg = DuplicateConfig::for_type("shared.Person");
        let new_data = map(&[("normalized_name", json!("alice smith"))]);
        let existing = map(&[("normalized_name", json!("alice smith"))]);
        let dup = check_duplicate(&cfg, &new_data, "e1", &existing);
        assert!(dup.is_some());
    }

    #[test]
    fn test_tag_exact_name() {
        let cfg = DuplicateConfig::for_type("shared.Tag");
        let new_data = map(&[("name", json!("rust"))]);
        let existing = map(&[("name", json!("rust"))]);
        assert!(check_duplicate(&cfg, &new_data, "e1", &existing).is_some());

        let other = map(&[("name", json!("python"))]);
        assert!(check_duplicate(&cfg, &new_data, "e2", &other).is_none());
    }

    #[test]
    fn test_normalize_name() {
        assert_eq!(normalize_name("  Alice   Smith  "), "alice smith");
        assert_eq!(normalize_name("BOB"), "bob");
        assert_eq!(normalize_name(""), "");
    }

    #[test]
    fn test_string_similarity_identical() {
        assert_eq!(string_similarity("hello", "hello"), 1.0);
    }

    #[test]
    fn test_string_similarity_case_insensitive() {
        assert!(string_similarity("Hello", "hello") >= 0.95);
    }

    #[test]
    fn test_string_similarity_different() {
        let sim = string_similarity("abcde", "xyz");
        assert!(sim < 0.5);
    }

    #[test]
    fn test_org_domain_match() {
        let cfg = DuplicateConfig::for_type("shared.Organization");
        let new_data = map(&[("domain", json!("example.com"))]);
        let existing = map(&[("domain", json!("example.com"))]);
        assert!(check_duplicate(&cfg, &new_data, "o1", &existing).is_some());
    }
}
