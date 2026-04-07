/// Capability Token System for Knowledge Graph access.
///
/// Tokens are HMAC-SHA256 signed structs that encode an application's
/// permitted scopes (read, write, relation, instance). The Graph Daemon
/// issues tokens at connection time and verifies them on every request.
///
/// See `docs/architecture/CAPABILITY-TOKENS.md` for the full specification.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use uuid::Uuid;

/// Current token format version.
const TOKEN_VERSION: u8 = 1;

// ---------------------------------------------------------------------------
// Token structure
// ---------------------------------------------------------------------------

/// A cryptographically signed capability token.
///
/// Encodes an application's permitted scopes for Knowledge Graph access.
/// Issued by the Graph Daemon, verified on every request via HMAC-SHA256.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityToken {
    // Header
    pub id: Uuid,
    pub version: u8,
    pub issued_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,

    // Identity
    pub app_id: String,
    pub pid: u32,

    // Scopes
    pub read_scopes: Vec<EntityScope>,
    pub write_scopes: Vec<EntityScope>,
    pub relation_scopes: Vec<RelationScope>,
    pub instance_scope: InstanceScope,

    // Signature (zeroed before signing/verification)
    #[serde(with = "serde_bytes")]
    pub signature: Vec<u8>,
}

/// Defines which fields of an entity type a token can access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityScope {
    /// Entity type (e.g. "system.File", "com.anki.Card").
    pub entity_type: String,
    /// Permitted fields. `None` means all fields.
    pub fields: Option<Vec<String>>,
    /// Fields excluded even when `fields` is `None`.
    pub exclude_fields: Vec<String>,
}

/// Defines a permitted relation creation between entity types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationScope {
    /// Source entity type.
    pub from: String,
    /// Target entity type.
    pub to: String,
    /// Relation type (e.g. "MENTIONS", "BELONGS_TO").
    pub relation_type: String,
}

/// Controls whether a token can access only its own entities or all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceScope {
    /// Can only access entities created by this app.
    Own,
    /// Can access all entities of permitted types.
    All,
}

// ---------------------------------------------------------------------------
// Token construction
// ---------------------------------------------------------------------------

impl CapabilityToken {
    /// Create a new unsigned token with the given scopes.
    pub fn new(
        app_id: String,
        pid: u32,
        read_scopes: Vec<EntityScope>,
        write_scopes: Vec<EntityScope>,
        relation_scopes: Vec<RelationScope>,
        instance_scope: InstanceScope,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            version: TOKEN_VERSION,
            issued_at: Utc::now(),
            expires_at: None,
            app_id,
            pid,
            read_scopes,
            write_scopes,
            relation_scopes,
            instance_scope,
            signature: vec![0u8; 32],
        }
    }

    /// Whether the token has expired.
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(exp) => Utc::now() > exp,
            None => false,
        }
    }

    /// Whether this token grants read access to the given entity type.
    pub fn can_read(&self, entity_type: &str) -> bool {
        scope_matches(&self.read_scopes, entity_type)
    }

    /// Whether this token grants write access to the given entity type.
    pub fn can_write(&self, entity_type: &str) -> bool {
        scope_matches(&self.write_scopes, entity_type)
    }

    /// Whether this token permits creating a relation of the given type.
    pub fn can_create_relation(&self, from: &str, to: &str, rel_type: &str) -> bool {
        self.relation_scopes.iter().any(|s| {
            s.from == from && s.to == to && s.relation_type == rel_type
        })
    }

    /// Returns the list of readable fields for an entity type, or None if
    /// all fields are permitted.
    pub fn readable_fields(&self, entity_type: &str) -> Option<Option<&[String]>> {
        self.read_scopes
            .iter()
            .find(|s| type_matches(&s.entity_type, entity_type))
            .map(|s| s.fields.as_deref())
    }
}

/// Check whether any scope in the list matches the given entity type.
fn scope_matches(scopes: &[EntityScope], entity_type: &str) -> bool {
    scopes.iter().any(|s| type_matches(&s.entity_type, entity_type))
}

/// Match an entity type against a scope pattern.
/// `"com.anki.*"` matches `"com.anki.Card"`, `"com.anki.Deck"`, etc.
/// `"system.File"` matches only `"system.File"`.
fn type_matches(pattern: &str, entity_type: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix(".*") {
        entity_type.starts_with(prefix) && entity_type[prefix.len()..].starts_with('.')
    } else {
        pattern == entity_type
    }
}

// ---------------------------------------------------------------------------
// Token signing
// ---------------------------------------------------------------------------

/// HMAC-SHA256 signer for capability tokens.
///
/// Generates a random 256-bit key at construction time. The key lives only
/// in memory and is never persisted. A new key is generated on every daemon
/// restart, invalidating all previously issued tokens.
pub struct TokenSigner {
    key: [u8; 32],
}

impl TokenSigner {
    /// Create a new signer with a random 256-bit HMAC key.
    pub fn new() -> Self {
        let mut key = [0u8; 32];
        getrandom::getrandom(&mut key).expect("failed to generate HMAC key");
        Self { key }
    }

    /// Create a signer with a specific key (for testing).
    #[cfg(test)]
    pub fn with_key(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Sign a token in place. Sets the `signature` field to the HMAC-SHA256
    /// of the MessagePack-serialized token (with signature zeroed).
    pub fn sign(&self, token: &mut CapabilityToken) {
        let bytes = self.signable_bytes(token);
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).unwrap();
        mac.update(&bytes);
        token.signature = mac.finalize().into_bytes().to_vec();
    }

    /// Verify that a token's signature is valid.
    pub fn verify(&self, token: &CapabilityToken) -> bool {
        let bytes = self.signable_bytes(token);
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).unwrap();
        mac.update(&bytes);
        mac.verify_slice(&token.signature).is_ok()
    }

    /// Serialize the token with the signature zeroed out, producing the
    /// bytes that are signed/verified.
    fn signable_bytes(&self, token: &CapabilityToken) -> Vec<u8> {
        let mut t = token.clone();
        t.signature = vec![0u8; 32];
        rmp_serde::to_vec(&t).expect("token serialization failed")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn test_read_scopes() -> Vec<EntityScope> {
        vec![
            EntityScope {
                entity_type: "system.File".into(),
                fields: Some(vec!["path".into(), "name".into()]),
                exclude_fields: vec![],
            },
            EntityScope {
                entity_type: "com.test.*".into(),
                fields: None,
                exclude_fields: vec!["secret".into()],
            },
        ]
    }

    fn test_write_scopes() -> Vec<EntityScope> {
        vec![EntityScope {
            entity_type: "com.test.Note".into(),
            fields: None,
            exclude_fields: vec![],
        }]
    }

    fn test_relation_scopes() -> Vec<RelationScope> {
        vec![RelationScope {
            from: "com.test.Note".into(),
            to: "system.File".into(),
            relation_type: "REFERENCES".into(),
        }]
    }

    fn make_token() -> CapabilityToken {
        CapabilityToken::new(
            "com.test".into(),
            1234,
            test_read_scopes(),
            test_write_scopes(),
            test_relation_scopes(),
            InstanceScope::Own,
        )
    }

    // ── HMAC key ──

    #[test]
    fn test_hmac_key_generation() {
        let signer1 = TokenSigner::new();
        let signer2 = TokenSigner::new();
        assert_ne!(signer1.key, signer2.key, "keys should be random");
    }

    // ── Signing and verification ──

    #[test]
    fn test_token_signature_valid() {
        let signer = TokenSigner::new();
        let mut token = make_token();
        signer.sign(&mut token);
        assert!(signer.verify(&token));
    }

    #[test]
    fn test_token_signature_tampered() {
        let signer = TokenSigner::new();
        let mut token = make_token();
        signer.sign(&mut token);
        token.app_id = "com.evil".into();
        assert!(!signer.verify(&token));
    }

    #[test]
    fn test_token_different_signer() {
        let signer1 = TokenSigner::new();
        let signer2 = TokenSigner::new();
        let mut token = make_token();
        signer1.sign(&mut token);
        assert!(!signer2.verify(&token), "different key should fail verification");
    }

    #[test]
    fn test_token_pid_tampered() {
        let signer = TokenSigner::new();
        let mut token = make_token();
        signer.sign(&mut token);
        token.pid = 9999;
        assert!(!signer.verify(&token));
    }

    // ── Read scopes ──

    #[test]
    fn test_can_read_exact_match() {
        let token = make_token();
        assert!(token.can_read("system.File"));
    }

    #[test]
    fn test_can_read_wildcard() {
        let token = make_token();
        assert!(token.can_read("com.test.Card"));
        assert!(token.can_read("com.test.Deck"));
    }

    #[test]
    fn test_can_read_denied() {
        let token = make_token();
        assert!(!token.can_read("system.Session"));
        assert!(!token.can_read("com.other.Thing"));
    }

    // ── Write scopes ──

    #[test]
    fn test_can_write() {
        let token = make_token();
        assert!(token.can_write("com.test.Note"));
        assert!(!token.can_write("system.File"));
        assert!(!token.can_write("com.test.Card"));
    }

    // ── Relation scopes ──

    #[test]
    fn test_can_create_relation() {
        let token = make_token();
        assert!(token.can_create_relation("com.test.Note", "system.File", "REFERENCES"));
        assert!(!token.can_create_relation("system.File", "com.test.Note", "REFERENCES"));
        assert!(!token.can_create_relation("com.test.Note", "system.File", "MENTIONS"));
    }

    // ── Instance scope ──

    #[test]
    fn test_instance_scope_own() {
        let token = make_token();
        assert_eq!(token.instance_scope, InstanceScope::Own);
    }

    #[test]
    fn test_instance_scope_all() {
        let token = CapabilityToken::new(
            "ai-daemon".into(),
            5678,
            test_read_scopes(),
            vec![],
            vec![],
            InstanceScope::All,
        );
        assert_eq!(token.instance_scope, InstanceScope::All);
    }

    // ── Expiration ──

    #[test]
    fn test_is_expired() {
        let mut token = make_token();
        assert!(!token.is_expired(), "no expiry = not expired");

        token.expires_at = Some(Utc::now() + Duration::hours(1));
        assert!(!token.is_expired(), "future expiry = not expired");

        token.expires_at = Some(Utc::now() - Duration::hours(1));
        assert!(token.is_expired(), "past expiry = expired");
    }

    // ── Type matching ──

    #[test]
    fn test_type_matching() {
        assert!(type_matches("system.File", "system.File"));
        assert!(!type_matches("system.File", "system.Session"));
        assert!(type_matches("com.test.*", "com.test.Card"));
        assert!(type_matches("com.test.*", "com.test.Deck"));
        assert!(!type_matches("com.test.*", "com.other.Card"));
        assert!(!type_matches("com.test.*", "com.test")); // no trailing dot
    }

    // ── Readable fields ──

    #[test]
    fn test_readable_fields() {
        let token = make_token();

        // system.File has explicit fields
        let fields = token.readable_fields("system.File");
        assert!(fields.is_some());
        let fields = fields.unwrap();
        assert!(fields.is_some());
        assert_eq!(fields.unwrap(), &["path", "name"]);

        // com.test.Card matches wildcard, fields = None (all)
        let fields = token.readable_fields("com.test.Card");
        assert!(fields.is_some());
        assert!(fields.unwrap().is_none());

        // system.Session not in scope
        assert!(token.readable_fields("system.Session").is_none());
    }

    // ── Serialization roundtrip ──

    #[test]
    fn test_msgpack_roundtrip() {
        let signer = TokenSigner::new();
        let mut token = make_token();
        signer.sign(&mut token);

        let bytes = rmp_serde::to_vec(&token).unwrap();
        let decoded: CapabilityToken = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(decoded.app_id, token.app_id);
        assert_eq!(decoded.pid, token.pid);
        assert_eq!(decoded.signature, token.signature);
        assert!(signer.verify(&decoded));
    }
}
