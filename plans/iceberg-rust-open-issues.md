# Apache Iceberg-Rust: Open Unassigned Issues for Contribution

> Generated: 2026-04-09
> Source: https://github.com/apache/iceberg-rust

Issues below are **open**, **unassigned**, and have **no active pull requests**. They are ordered by estimated complexity (simplest first).

---

## Low Complexity

### 1. [#1354] Missing `lower_bound`/`upper_bound` incorrectly treated as all-NULL

- **Labels:** bug
- **Summary:** When partition statistics are missing `lower_bound` or `upper_bound`, the code returns `ROWS_CANNOT_MATCH` instead of `ROWS_MIGHT_MATCH`. This incorrectly excludes partitions from scans.
- **What to change:** In the manifest evaluator, change `None => ROWS_CANNOT_MATCH` to `None => ROWS_MIGHT_MATCH` across multiple comparison operators (`<`, `<=`, `>`, `>=`). The Java implementation already handles this correctly.
- **Why it's simple:** Straightforward logic fix in a single file with clear before/after behavior. Needs test coverage.
- **Link:** https://github.com/apache/iceberg-rust/issues/1354

### 2. [#2135] REST catalog CreateTableRequest sends explicit null values, causing 400 errors

- **Labels:** bug
- **Summary:** Optional fields like `location`, `partition_spec`, `write_order`, `stage_create` are serialized as `null` instead of being omitted. Polaris and other REST catalogs reject these with HTTP 400.
- **What to change:** Add `#[serde(skip_serializing_if = "Option::is_none")]` to optional fields in `CreateTableRequest`, `UnboundPartitionSpec`, and `UnboundPartitionField`.
- **Why it's simple:** Pure annotation change. Two prior PRs (#2136, #2155) were closed due to inactivity (not rejected) — the approach was approved. Needs a unit test for serde round-trip.
- **Link:** https://github.com/apache/iceberg-rust/issues/2135

### 3. [#1697] StaticTable::from_metadata_file() fails for v1-to-v2 upgraded tables

- **Labels:** bug
- **Summary:** Tables upgraded from format v1 to v2 fail to parse because v1 snapshots lack the optional `sequence-number` field. The deserializer errors instead of defaulting to 0.
- **What to change:** Add `#[serde(default)]` or explicit default of `0` for the `sequence_number` field in the snapshot deserialization struct, matching the Iceberg spec.
- **Why it's simple:** Small deserialization fix. Clear specification guidance.
- **Link:** https://github.com/apache/iceberg-rust/issues/1697

### 4. [#1720] Unbounded object_cache size

- **Labels:** bug
- **Summary:** The moka cache weigher uses `size_of_val()` which only measures stack size, not heap-allocated data in `ManifestList` and `Manifest` entries. Actual memory exceeds `max_capacity`.
- **What to change:** Update the weigher closure in `object_cache.rs` (lines ~63-69) to include estimated heap allocation sizes (~2KB for ManifestList, ~8KB for Manifest).
- **Why it's simple:** Localized change in a single closure. No architectural changes needed.
- **Link:** https://github.com/apache/iceberg-rust/issues/1720

### 5. [#2245] Remove `configured_scheme` from OpenDalStorage::{S3, Azdls}

- **Labels:** enhancement
- **Summary:** The Storage API now accepts fully-qualified locations, making the `configured_scheme` field unnecessary. The scheme-to-storage mapping should also support multiple schemes per storage (e.g., "s3", "s3a", "s3n" all map to S3).
- **What to change:** Remove the `configured_scheme` field from S3 and Azdls storage structs, and update `OpenDalResolvingStorage` to map multiple schemes to the same storage instance.
- **Why it's simple:** Structural cleanup / refactoring with clear scope.
- **Link:** https://github.com/apache/iceberg-rust/issues/2245

---

## Low-to-Medium Complexity

### 6. [#2028] Replace `properties` field in TableMetadata with `TableProperties`

- **Labels:** bug, good first issue
- **Summary:** `TableMetadata.properties` is `HashMap<String, String>` but should be the typed `TableProperties` struct. The `table_properties()` method reconstructs the typed wrapper on every call.
- **What to change:** Change the field type in `crates/iceberg/src/spec/table_metadata.rs`, update the accessor to return a reference, update serde, and fix all downstream usages.
- **Why it's manageable:** Labeled "good first issue". A prior PR (#2037) was closed due to inactivity — reviewer feedback exists to guide the approach. Needs attention to serialization details.
- **Link:** https://github.com/apache/iceberg-rust/issues/2028

### 7. [#1862] Make `CatalogBuilder.0` more explicit

- **Labels:** enhancement
- **Summary:** Accessing catalog configuration via tuple index `.0` is unclear. Should be replaced with a named method like `.catalog_config()`.
- **What to change:** Add getter methods to relevant `CatalogBuilder` structs and update all `.0` access sites.
- **Why it's manageable:** API ergonomics improvement. Prior PR (#1873) closed due to inactivity — reviewer feedback available. Touches multiple catalog builder files.
- **Link:** https://github.com/apache/iceberg-rust/issues/1862

### 8. [#2184] Add `iceberg.schema` to Parquet footer for engine compatibility

- **Labels:** bug
- **Summary:** Parquet files written by iceberg-rust lack the `iceberg.schema` metadata in the file footer. Snowflake and other engines cannot read these files.
- **What to change:** When writing Parquet files, serialize the Iceberg schema JSON and add it as `iceberg.schema` key-value metadata in the Parquet footer. Java implementation provides a reference.
- **Why it's manageable:** Clear requirement with Java reference. Localized to the Parquet writer.
- **Link:** https://github.com/apache/iceberg-rust/issues/2184

### 9. [#2068] column 'iceberg_type' does not exist (SQL catalog v0 schema compatibility)

- **Labels:** bug
- **Summary:** iceberg-rust's SQL catalog expects v1 schema (with `iceberg_type` column), but catalogs created by PyIceberg use v0 schema (without it). This causes connection failures.
- **What to change:** Add detection for whether the `iceberg_type` column exists. Use conditional SQL queries based on schema version. Optionally support auto-migration from v0 to v1.
- **Why it's manageable:** Active community discussion (16 comments) with clear design direction. Java implementation provides a migration reference.
- **Link:** https://github.com/apache/iceberg-rust/issues/2068

---

## Medium Complexity

### 10. [#1529] Schema evolution error when querying Parquet files with different schema versions

- **Labels:** bug
- **Summary:** Reading an Iceberg table after schema evolution fails if older Parquet files lack newly added columns. The reader throws a schema mismatch error instead of treating missing fields as null.
- **What to change:** Modify the Parquet reader to accept schema mismatches for optional columns added via schema evolution. Inject null values for missing columns during reads.
- **Why it's medium:** Touches the core Parquet reader logic. Requires careful handling of schema versioning. Java/Python implementations handle this correctly for reference.
- **Link:** https://github.com/apache/iceberg-rust/issues/1529

### 11. [#2261] Implement ObjectStoreStorage::Memory

- **Labels:** enhancement
- **Summary:** Add in-memory storage backend for the `object_store`-based Storage API. Sub-issue of #2258.
- **What to change:** Implement the `Storage` trait for an in-memory object store variant. Follow existing patterns from other ObjectStoreStorage implementations.
- **Why it's medium:** Requires implementing a trait with multiple methods. Good for learning the storage abstraction layer.
- **Link:** https://github.com/apache/iceberg-rust/issues/2261

### 12. [#2262] Implement ObjectStoreStorage::LocalFs

- **Labels:** enhancement
- **Summary:** Add local filesystem storage backend for the `object_store`-based Storage API. Sub-issue of #2258.
- **What to change:** Implement the `Storage` trait for local filesystem using `object_store::local::LocalFileSystem`.
- **Why it's medium:** Similar scope to #2261. Useful for local development and testing workflows.
- **Link:** https://github.com/apache/iceberg-rust/issues/2262

---

## Notes

- Issues #2312 (S3 Tables URL scheme) already has PR #2313 in progress — skipped.
- Issues #1731 (compression settings) has PRs #1876 and #2288 merged — mostly resolved.
- Issue #1845 (schema visitor) appears to have been addressed by subsequent work.
- Issue #1934 (ArrowReaderOptions) has active PR #2003 — skipped.
- Issue #1780 (sqllogictest catalog config) had multiple PRs addressing it — may be partially resolved.

### How to Claim an Issue

1. Comment on the issue expressing interest
2. Wait for a maintainer to assign it to you
3. Reference the issue number in your PR title (e.g., `fix: skip null serialization in CreateTableRequest (#2135)`)
