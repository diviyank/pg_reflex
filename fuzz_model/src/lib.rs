//! pgrx-free fuzz model shared by the in-backend `#[pg_test]` gate and the
//! standalone `fuzz_sweep` discovery binary. Produces SQL strings and case
//! descriptors only — no SPI, no pgrx runtime dependency.

#[cfg(test)]
mod smoke {
    #[test]
    fn crate_builds() {
        assert_eq!(2 + 2, 4);
    }
}
