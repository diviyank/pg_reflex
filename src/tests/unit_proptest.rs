use proptest::prelude::*;

proptest! {
    /// Any string with characters outside [a-zA-Z0-9_.] should be rejected
    #[test]
    fn validate_rejects_unsafe_chars(s in "[a-zA-Z_][a-zA-Z0-9_.]{0,20}[^a-zA-Z0-9_.]+") {
        assert!(crate::validate_view_name(&s).is_err());
    }

    /// Any valid identifier (letter/underscore start, alphanumeric/underscore/period body,
    /// no consecutive dots, no trailing dot) should be accepted
    #[test]
    fn validate_accepts_safe_names(s in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        assert!(crate::validate_view_name(&s).is_ok());
    }

    /// Schema-qualified names (one dot, valid parts) should be accepted
    #[test]
    fn validate_accepts_schema_qualified(
        schema in "[a-zA-Z_][a-zA-Z0-9_]{0,10}",
        name in "[a-zA-Z_][a-zA-Z0-9_]{0,10}",
    ) {
        let qualified = format!("{}.{}", schema, name);
        assert!(crate::validate_view_name(&qualified).is_ok());
    }

    /// Empty string should always be rejected
    #[test]
    fn validate_rejects_empty(s in "[ \t\n]*") {
        if s.is_empty() {
            assert!(crate::validate_view_name(&s).is_err());
        }
    }
}
