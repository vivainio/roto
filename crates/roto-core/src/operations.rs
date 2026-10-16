//! Which operations of a generated service trait a service actually implements.
//!
//! `#[implements_operations]` on `impl Service for X` records the methods of the block, so
//! the supported set is the impl itself; operations left to the trait's `NotImplemented`
//! defaults are not in it.

/// Implemented by `#[implements_operations]`.
pub trait ImplementsOperations {
    /// The snake_case trait methods the `impl` block defines.
    fn implemented_methods() -> &'static [&'static str];
}

/// The operations from a generated `OPERATIONS` table whose trait method is in `methods`.
pub fn supported_operations(
    operations: &[(&'static str, bool)],
    methods: &[&str],
) -> Vec<&'static str> {
    operations
        .iter()
        .filter(|(name, codec)| *codec && methods.contains(&snake(name).as_str()))
        .map(|(name, _)| *name)
        .collect()
}

/// The method name `roto-codegen` gives an operation (`AssumeRoleWithSAML` -> `assume_role_with_saml`).
fn snake(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());
            if prev.is_lowercase() || prev.is_ascii_digit() || (prev.is_uppercase() && next_lower) {
                out.push('_');
            }
        }
        out.extend(c.to_lowercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_matches_codegen() {
        assert_eq!(snake("GetCallerIdentity"), "get_caller_identity");
        assert_eq!(snake("AssumeRoleWithSAML"), "assume_role_with_saml");
    }

    #[test]
    fn only_implemented_codec_operations_are_supported() {
        let ops = [("GetThing", true), ("PutThing", true), ("DropThing", false)];
        assert_eq!(
            supported_operations(&ops, &["get_thing", "drop_thing"]),
            ["GetThing"]
        );
    }
}
