# Crate structure

Overview of crates.

## Leaf crates

Leaf crates are crates that live on the end of the dependency tree. Leaf crates
should never import an integration crate, and should strive to import a minimal
amount of other leaf crates.

- `arrow_util`: Utilities around detecting terminal features
  - WASM: not yet (datafusion, tokio, parquet)
- `terminal_util`: Utilities around detecting terminal features
  - WASM: no

## Integration crates

Integration crates are crates that import a bunch of leaf crates. Integration
crates should strive to import a minimal amount of other integration crates.
