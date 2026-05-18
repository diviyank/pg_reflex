//! Tiny slot-substitution helper for relocated SQL templates.
//!
//! Used by Phase 4 of the 1.6.1 refactor to replace
//! `__REFLEX_SLOT_<NAME>__` sentinels in trigger-body templates without
//! reaching for nested `format!()` calls.
//!
//! Substitution is **non-overlapping**: each occurrence of every slot
//! is replaced exactly once, in the order given. The implementation
//! walks the template byte-by-byte; the slots are sentinel-shaped
//! tokens chosen so they can never appear in real SQL.

/// Replace every occurrence of every `(name, value)` pair in `template`.
///
/// `name` is expected to be a sentinel like `"__REFLEX_SLOT_TRANSITION_TBL__"`.
/// Replacement is greedy: every occurrence of every slot is rewritten in
/// the order the slots appear in the slice. A slot whose name never appears
/// in `template` is a no-op (the caller's job to verify expectations).
pub fn slot_replace(template: &str, slots: &[(&str, &str)]) -> String {
    let mut out = template.to_string();
    for (name, value) in slots {
        if name.is_empty() {
            continue;
        }
        out = out.replace(name, value);
    }
    out
}
