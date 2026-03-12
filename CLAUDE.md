# CLAUDE.md — Apache Iceberg Rust

## Build & Development

```bash
make build          # Compile all workspace crates with all features
make check          # Run all lints: fmt, clippy, toml format, unused deps
make check-fmt      # Verify rustfmt formatting
make check-clippy   # Clippy with -D warnings (warnings are errors)
make check-toml     # Verify TOML formatting (taplo)
make cargo-machete  # Detect unused dependencies
make check-msrv     # Verify MSRV (1.92) compatibility
```

## Testing

```bash
make unit-test      # Unit + doc tests (no Docker needed)
make doc-test       # Documentation tests only
make test           # Full test suite (requires Docker)
make docker-up      # Start Docker containers (MinIO, REST catalog, HMS, Glue, PostgreSQL)
make docker-down    # Stop Docker containers
```

Run a single test:
```bash
cargo test -p iceberg --lib -- test_name
cargo nextest run -p iceberg test_name
```

Tests use `rstest` for parameterization (`#[rstest]` + `#[case]`), `#[tokio::test]` for async, and `expect_test` for snapshot assertions. The `iceberg_test_utils` crate (`crates/test_utils/`) provides shared catalog setup and assertion helpers.

## Workspace Structure

| Path | Purpose |
|------|---------|
| `crates/iceberg` | Core library — types, specs, readers, writers, transforms |
| `crates/catalog/{rest,hms,glue,sql,s3tables}` | Catalog implementations |
| `crates/catalog/loader` | Unified catalog loader |
| `crates/storage/opendal` | OpenDAL storage backend |
| `crates/integrations/datafusion` | DataFusion query engine integration |
| `crates/integrations/cache-moka` | Moka-based caching layer |
| `crates/test_utils` | Shared test utilities |
| `crates/integration_tests` | Integration test suite |
| `crates/examples` | Example applications |
| `bindings/python` | Python bindings (excluded from workspace, separate build) |

## Key Traits

- **`Catalog`** (`crates/iceberg/src/catalog/mod.rs`) — namespace and table management (list, create, load, drop). Uses `#[async_trait]`.
- **`Storage`** (`crates/iceberg/src/io/storage/mod.rs`) — file I/O abstraction (read, write, delete). Tagged with `#[typetag::serde]` for serialization.
- **`IcebergWriter`** / **`IcebergWriterBuilder`** (`crates/iceberg/src/writer/mod.rs`) — logical-level writers (data files, delete files).
- **`FileWriter`** / **`FileWriterBuilder`** (`crates/iceberg/src/writer/mod.rs`) — physical file format writers (Parquet).

## Code Quality

- **Nightly toolchain** (`nightly-2025-10-27` via `rust-toolchain.toml`) — used only for rustfmt and clippy during development.
- **MSRV**: 1.92, edition 2024. Checked in CI via `make check-msrv`.
- **rustfmt**: groups imports by `StdExternalCrate`, module-level granularity, trailing commas in vertical lists.
- **Clippy**: `-D warnings` — all warnings are errors.
- **TOML**: formatted with taplo (80-column, 2-space indent).
- **Cargo.lock**: committed for reproducible builds.

## Conventions

- **PR titles**: [Conventional Commits](https://www.conventionalcommits.org/) — e.g. `feat(schema): Add field`, `fix(reader): Handle nulls`, `chore(deps): Bump crate`.
- **Merge strategy**: Squash merge.
- **License headers**: Apache 2.0 on all source files, enforced by `skywalking-eyes` in CI.
- **PR size**: Keep under 300–500 lines of diff when possible.
- **During review**: Use `git merge` to update branch, never force push.
