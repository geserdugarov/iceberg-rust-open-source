<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Iceberg Rust — Agent Instructions

Repository-specific guidance for automated agents. Keep edits minimal and
follow existing conventions in `CONTRIBUTING.md` and `README.md`.

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
| `bindings/python` | Python bindings (workspace member; wheel built via `maturin`/`pyproject.toml`) |
| `dev/` | Docker Compose stack for integration tests |
| `docs/`, `website/` | Project documentation (mdBook) |
| `plans/` | Design notes and implementation plans |

Key traits live in `crates/iceberg/src/`:

- `Catalog` — `catalog/mod.rs`: namespace and table management.
- `Storage` — `io/storage/mod.rs`: file I/O abstraction.
- `IcebergWriter` / `FileWriter` — `writer/mod.rs`: logical and physical writers.

## CI / Local Checks

CI runs the same `make` targets used locally. Run before pushing:

```bash
make check          # fmt + clippy + toml + unused deps (warnings are errors)
make unit-test      # unit + doc tests, no Docker
make test           # full suite; starts Docker stack via `make docker-up`
make check-msrv     # verify MSRV (1.92) compatibility
```

Individual targets: `make build`, `make check-fmt`, `make check-clippy`,
`make check-toml`, `make cargo-machete`, `make doc-test`,
`make docker-up` / `make docker-down`.

Run a single test:

```bash
cargo test -p iceberg --lib -- test_name
cargo nextest run -p iceberg test_name
```

Toolchain: edition 2024, MSRV 1.92 (`Cargo.toml`); nightly pinned in
`rust-toolchain.toml` is used only for `rustfmt` and `clippy`.

## Conventions

- **Commits / PR titles**: [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat(scope):`, `fix(scope):`, `chore(deps):`, `docs:`, …).
- **Merge strategy**: squash merge; update branches with `git merge`, never force push.
- **License headers**: Apache 2.0 on every source file, enforced by
  `skywalking-eyes` in CI (`.licenserc.yaml`).
- **PR size**: aim for under 300–500 lines of diff.

## Security Model

When assessing potential vulnerabilities or calibrating automated security
findings, use [`SECURITY-THREAT-MODEL.md`](SECURITY-THREAT-MODEL.md) as the
authoritative description of this repository's security boundaries, trust
assumptions, and non-boundaries.
