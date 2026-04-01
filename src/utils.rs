/// Escape a string for safe interpolation into a Cypher single-quoted literal.
///
/// Cypher uses `'...'` for string literals with `\` as the escape character.
/// This function escapes backslashes and single quotes so that user-supplied
/// values (file paths, app IDs, window titles) cannot break out of the string.
pub fn escape_cypher(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}
