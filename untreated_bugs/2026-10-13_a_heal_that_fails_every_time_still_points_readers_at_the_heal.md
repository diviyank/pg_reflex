# A queued heal that fails on every sweep still prints the heal remedy to its readers — S3 (remedy loop)

Found by the 1.12.1 review (`0f8054a`, `heal_stale_reasons_by_imv` in `src/heal.rs`); not tested.

## What happens

An IMV whose readers are reported stale through the heal queue gets the remedy
`SELECT reflex_heal_ignored_sources('<top-level IMV>');` when every origin reaching it counts as
drained by the sweep. A queued origin whose heal fails each time (its rows keep `last_error`) still
counts as drained, so its readers keep being told to run a heal that leaves them flagged. The origin's
own status names the failure.

## Fix direction

Treat an origin whose queue rows carry `last_error` as not drained: print
"repair '<origin>' first (see its stale_reason)" for its readers, as for an unhealable origin.
Test: a heal that raises on every attempt (e.g. a reader whose refresh a trigger refuses) — the reader's
printed remedy must name the origin, and fixing the origin then running the heal must clear both.
