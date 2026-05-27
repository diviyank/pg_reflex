//! pgrx-free fuzz model shared by the in-backend `#[pg_test]` gate and the
//! standalone `fuzz_sweep` discovery binary. Produces SQL strings and case
//! descriptors only — no SPI, no pgrx runtime dependency.

pub mod model;
pub mod render;
pub mod generate;
