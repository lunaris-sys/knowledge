/// Merge suggestions for duplicate shared entities.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::duplicate::DuplicateCandidate;

/// A suggestion to merge two entities that appear to be duplicates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeSuggestion {
    pub id: String,
    pub entity_type: String,
    pub source_id: String,
    pub target_id: String,
    pub match_score: f64,
    pub match_fields: Vec<String>,
    pub status: SuggestionStatus,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

/// Current status of a merge suggestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SuggestionStatus {
    Pending,
    Accepted,
    Rejected,
    Expired,
}

impl MergeSuggestion {
    /// Create a new pending suggestion from a duplicate candidate.
    pub fn new(
        entity_type: &str,
        source_id: &str,
        candidate: &DuplicateCandidate,
        created_by: &str,
    ) -> Self {
        Self {
            id: uuid::Uuid::now_v7().to_string(),
            entity_type: entity_type.into(),
            source_id: source_id.into(),
            target_id: candidate.existing_id.clone(),
            match_score: candidate.match_score,
            match_fields: candidate.match_fields.clone(),
            status: SuggestionStatus::Pending,
            created_at: Utc::now(),
            created_by: created_by.into(),
        }
    }
}

/// Action to take after accepting or rejecting a merge.
#[derive(Debug)]
pub enum MergeAction {
    /// Delete source, keep target, re-point relations.
    Merge {
        delete_id: String,
        keep_id: String,
        update_relations: bool,
    },
    /// Keep both entities as separate (mark not-duplicate).
    KeepBoth {
        mark_not_duplicate: bool,
    },
}

/// Build the action for accepting a merge suggestion.
pub fn accept_merge(suggestion: &MergeSuggestion) -> MergeAction {
    MergeAction::Merge {
        delete_id: suggestion.source_id.clone(),
        keep_id: suggestion.target_id.clone(),
        update_relations: true,
    }
}

/// Build the action for rejecting a merge suggestion.
pub fn reject_merge(_suggestion: &MergeSuggestion) -> MergeAction {
    MergeAction::KeepBoth {
        mark_not_duplicate: true,
    }
}

/// Cypher to list pending suggestions.
pub fn pending_suggestions_query(entity_type: Option<&str>, limit: usize) -> String {
    match entity_type {
        Some(t) => format!(
            "MATCH (s:MergeSuggestion) WHERE s.status = 'pending' AND s.entity_type = '{}' \
             RETURN s ORDER BY s.created_at DESC LIMIT {}",
            t, limit,
        ),
        None => format!(
            "MATCH (s:MergeSuggestion) WHERE s.status = 'pending' \
             RETURN s ORDER BY s.created_at DESC LIMIT {}",
            limit,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::duplicate::DuplicateCandidate;

    fn candidate() -> DuplicateCandidate {
        DuplicateCandidate {
            existing_id: "existing-1".into(),
            match_score: 0.95,
            match_fields: vec!["email".into()],
        }
    }

    #[test]
    fn test_create_suggestion() {
        let s = MergeSuggestion::new("shared.Person", "new-1", &candidate(), "com.test");
        assert_eq!(s.entity_type, "shared.Person");
        assert_eq!(s.source_id, "new-1");
        assert_eq!(s.target_id, "existing-1");
        assert_eq!(s.status, SuggestionStatus::Pending);
        assert!(!s.id.is_empty());
    }

    #[test]
    fn test_accept_merge() {
        let s = MergeSuggestion::new("shared.Person", "new-1", &candidate(), "com.test");
        match accept_merge(&s) {
            MergeAction::Merge { delete_id, keep_id, update_relations } => {
                assert_eq!(delete_id, "new-1");
                assert_eq!(keep_id, "existing-1");
                assert!(update_relations);
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn test_reject_merge() {
        let s = MergeSuggestion::new("shared.Person", "new-1", &candidate(), "com.test");
        match reject_merge(&s) {
            MergeAction::KeepBoth { mark_not_duplicate } => {
                assert!(mark_not_duplicate);
            }
            _ => panic!("expected KeepBoth"),
        }
    }

    #[test]
    fn test_pending_query_with_type() {
        let q = pending_suggestions_query(Some("shared.Person"), 10);
        assert!(q.contains("shared.Person"));
        assert!(q.contains("LIMIT 10"));
    }

    #[test]
    fn test_pending_query_all() {
        let q = pending_suggestions_query(None, 50);
        assert!(!q.contains("entity_type"));
        assert!(q.contains("LIMIT 50"));
    }
}
