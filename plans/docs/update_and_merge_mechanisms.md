# Iceberg Rust UPDATE and MERGE INTO (Upsert) Mechanisms

This document describes the state of UPDATE and MERGE INTO support in the Rust Iceberg implementation and how it compares to the Java implementation.

**Related docs:** [architecture_overview.md](architecture_overview.md) | [write_path_callstack.md](write_path_callstack.md) | [read_path_callstack.md](read_path_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md) | [compaction_callstack.md](compaction_callstack.md)

---

## 1. Current Status: LARGELY NOT IMPLEMENTED

The Rust Iceberg implementation does **not yet support row-level UPDATE or MERGE INTO operations** at the SQL/DML level. Neither Copy-on-Write (CoW) nor Merge-on-Read (MoR) execution paths for UPDATE or MERGE exist.

The following are missing:
- No `OverwriteFiles` transaction action (needed for CoW UPDATE/MERGE)
- No `RowDelta` transaction action (needed for MoR UPDATE/MERGE)
- No position delete file writing (needed for MoR)
- No DataFusion integration for row-level DML operations
- No row-level conflict validation or `SERIALIZABLE`/`SNAPSHOT` isolation
  level support (the catalog already enforces optimistic
  `UuidMatch` / `RefSnapshotIdMatch` requirements as retryable
  `CatalogCommitConflicts` — what's missing is UPDATE/MERGE-specific
  validation of scanned/affected files and partitions)

---

## 2. What Exists

While UPDATE/MERGE are not implemented, the Rust codebase has building blocks that would be used:

### 2.1 Transaction Framework

```
Transaction                                         [crates/iceberg/src/transaction/mod.rs]
  │
  ├─> Holds a Table and a list of BoxedTransactionAction
  ├─> commit(catalog) with retry + exponential backoff (via `backon`)
  │
  └─> Available actions:
        ├── FastAppendAction            add new data files (append-only)
        ├── UpdatePropertiesAction      modify table properties
        ├── UpgradeFormatVersionAction  V1 → V2 → V3
        ├── UpdateStatisticsAction      update stats files
        ├── UpdateLocationAction        change table location
        ├── ReplaceSortOrderAction      change sort order
        └── UpdateSchemaAction          add/delete columns (top-level / nested)

  Missing actions for UPDATE/MERGE:
        ├── OverwriteFilesAction        delete old + add new files (CoW)
        ├── RowDeltaAction              add data + delete files atomically (MoR)
        └── RewriteFilesAction          swap files for compaction
```

### 2.2 Delete File Infrastructure (Read-Side)

```
The read path can already apply delete files during scans:

DeleteFileIndex                                     [crates/iceberg/src/delete_file_index.rs]
  └─> Associates delete files with data files during planning

DeleteFilter                                        [crates/iceberg/src/arrow/delete_filter.rs]
  └─> Applies position deletes + equality deletes during reading

DeleteVector                                        [crates/iceberg/src/delete_vector.rs]
  └─> RoaringTreemap bitmap for position delete tracking

CachingDeleteFileLoader                             [crates/iceberg/src/arrow/caching_delete_file_loader.rs]
  └─> Loads and caches delete files

This means: If Parquet position deletes or equality delete files were
written by another system (e.g., Java Spark), the Rust reader would
correctly apply them. Puffin-format deletion vectors (V3) are NOT yet
loaded — `CachingDeleteFileLoader` has an explicit TODO for the
"Delete Vector loader from Puffin files" path.
```

### 2.3 Equality Delete Writer

```
EqualityDeleteFileWriter                            [crates/iceberg/src/writer/base_writer/equality_delete_writer.rs]
  │
  └─> Can write equality delete files (Parquet format).
      Equality-id field validation rejects nested and floating-point
      types, but the spec's "no nullable" rule for identifier fields
      is NOT yet enforced here.

  This is the only delete write capability currently available.
  (The base_writer module docs still mention `PositionDeleteFileWriter`,
   but only `data_file_writer` and `equality_delete_writer` are exposed.)
```

### 2.4 DataFusion Integration

```
IcebergTableProvider                                [crates/integrations/datafusion/src/table/mod.rs]
  │
  ├─> scan() → IcebergTableScan                       read operations (IMPLEMENTED)
  ├─> insert_into(state, input, InsertOp) → IcebergCommitExec
  │                                                   append operations (IMPLEMENTED)
  │     The `InsertOp` argument is currently ignored (parameter is
  │     `_insert_op`) — only the append path is wired up;
  │     `Overwrite` and `Replace` semantics are not supported.
  │     The returned plan is `IcebergCommitExec` wrapping a
  │     `CoalescePartitionsExec` over `IcebergWriteExec`.
  │
  └─> Row-level operations (UPDATE, DELETE, MERGE):
        NOT IMPLEMENTED
        DataFusion 53.1.0's `TableProvider` exposes `update(...)` and
        `delete_from(...)` hooks that the physical planner can dispatch
        to, but `IcebergTableProvider` does not override them, so
        UPDATE and DELETE fall back to DataFusion's default
        "unsupported" error.
        MERGE INTO is not yet exposed via `TableProvider` in
        DataFusion 53.1.0 and remains unsupported.
        A Spark-style row-level DML framework (e.g. SupportsDeleteV2 /
        SupportsRowLevelOperations) would still need to be built on
        top of these hooks for full CoW/MoR semantics.
```

---

## 3. How UPDATE Works in Java (Reference)

For context, here is how Java implements UPDATE using both strategies:

### 3.1 Copy-on-Write UPDATE (Java)

```
UPDATE table SET value = 'X' WHERE id = 42
       │
       V
SparkCopyOnWriteOperation
  │
  ├─> SCAN: Read affected data files with metadata columns (_file, _pos)
  │
  ├─> EXECUTE: For each row in affected files:
  │     ├─> if WHERE matches: apply SET expressions (new values)
  │     └─> else: pass through unchanged
  │     Output ALL rows (entire file rewritten)
  │
  ├─> WRITE: New Parquet files with all rows (modified + unmodified)
  │
  └─> COMMIT: OverwriteFiles
        .deleteFile(old_files)
        .addFile(new_files)
        .commit()

Result: Old data files replaced with new ones. No delete files.
```

### 3.2 Merge-on-Read UPDATE (Java)

```
UPDATE table SET value = 'X' WHERE id = 42
       │
       V
SparkPositionDeltaOperation
  │
  ├─> SCAN: Read target table with _file + _pos metadata
  │
  ├─> EXECUTE: Decompose UPDATE into DELETE + INSERT:
  │     For matching row at (data-file-001, pos=17):
  │       DELETE marker: (_file="data-file-001", _pos=17)
  │       INSERT row:    {id=42, value="X"}
  │
  ├─> WRITE:
  │     Delete writer → position delete file (or DV)
  │     Data writer   → new data file with updated row
  │
  └─> COMMIT: RowDelta
        .addDeletes(delete_file)
        .addRows(new_data_file)
        .commit()

Result: Original file untouched. Delete file marks old row.
        New file contains updated row. Applied at read time.
```

---

## 4. How MERGE INTO Works in Java (Reference)

### 4.1 Copy-on-Write MERGE (Java)

```
MERGE INTO target t USING source s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET t.value = s.value
  WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
       │
       V
SparkCopyOnWriteOperation
  │
  ├─> SCAN: Read affected target files
  │
  ├─> EXECUTE:
  │     1. JOIN target ⋈ source ON t.id = s.id
  │     2. Evaluate clauses (first match wins):
  │        WHEN MATCHED → UPDATE: modify row
  │        WHEN NOT MATCHED → INSERT: new row
  │     3. Output ALL rows from affected files
  │
  ├─> WRITE: New data files
  │
  └─> COMMIT: OverwriteFiles
```

### 4.2 Merge-on-Read MERGE (Java)

```
MERGE INTO target t USING source s ON t.id = s.id
  WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
  WHEN MATCHED THEN UPDATE SET t.value = s.value
  WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
       │
       V
SparkPositionDeltaOperation
  │
  ├─> EXECUTE:
  │     WHEN MATCHED AND DELETE → position delete marker only
  │     WHEN MATCHED AND UPDATE → position delete + new data row
  │     WHEN NOT MATCHED → new data row (insert only)
  │
  ├─> WRITE:
  │     Delete writer: position deletes / DVs for matched rows
  │     Data writer:   new data files for inserts + update-inserts
  │
  └─> COMMIT: RowDelta
```

---

## 5. What Would Be Needed for Rust Implementation

### 5.1 OverwriteFiles Action (for CoW)

```
OverwriteFilesAction                                (hypothetical)
  │
  ├─> deleted_files: Vec<DataFile>       files to remove
  ├─> added_files: Vec<DataFile>         files to add
  ├─> validation:
  │     validate_from_snapshot(snapshot_id)
  │     validate_no_conflicting_data()     (serializable)
  │     validate_no_conflicting_deletes()  (both levels)
  │
  └─> commit() → ActionCommit:
        updates: [
          AddSnapshot(new_snapshot with operation="overwrite"),
          SetSnapshotRef(main → new_snapshot),
        ]
        requirements: [
          TableRequirement::UuidMatch { uuid },
          TableRequirement::RefSnapshotIdMatch {
            ref: "main",
            snapshot_id: current,
          },
        ]
```

### 5.2 RowDelta Action (for MoR)

```
RowDeltaAction                                      (hypothetical)
  │
  ├─> added_data_files: Vec<DataFile>    new data rows
  ├─> added_delete_files: Vec<DataFile>  delete markers
  ├─> referenced_data_files: Vec<String> files referenced by deletes
  ├─> validation:
  │     validate_data_files_exist(referenced_files)
  │     validate_no_conflicting_delete_files()
  │     validate_no_conflicting_data_files()  (serializable only)
  │
  └─> commit() → ActionCommit:
        updates: [
          AddSnapshot(new_snapshot with operation="overwrite"),
          SetSnapshotRef(main → new_snapshot),
        ]
```

### 5.3 Position Delete Writer

```
PositionDeleteFileWriter                            (hypothetical)
  │
  ├─> Writes Parquet files with schema:
  │     file_path: string (required)
  │     pos: long (required)
  │     Sorted by (file_path ASC, pos ASC)
  │
  └─> close() → Vec<DataFile> with content=PositionDeletes
```

### 5.4 Deletion Vector Writer

```
DeletionVectorWriter                                (hypothetical)
  │
  ├─> Accumulate positions per data file path:
  │     HashMap<String, RoaringTreemap>
  │
  ├─> Merge with previous DVs (if updating existing DV)
  │
  └─> Write Puffin file (PuffinWriter already exists in
        crates/iceberg/src/puffin/writer.rs). Current API:
        PuffinWriter::add(blob, compression_codec)
          where blob is a `Blob` with
            type=DELETION_VECTOR_V1,
            properties={"referenced-data-file": path},
            data=bitmap.serialize().
        Return DataFile with content=PositionDeletes, format=Puffin.

  The Puffin blob types `DELETION_VECTOR_V1` and
  `APACHE_DATASKETCHES_THETA_V1` are defined in
  crates/iceberg/src/puffin/blob.rs.
```

### 5.5 DataFusion DML Integration

```
For full UPDATE/MERGE support in DataFusion:

1. DataFusion would need row-level DML execution planning
   (similar to Spark's SupportsDeleteV2)

2. IcebergTableProvider would need:
   ├─> Row identification columns (_file, _pos)
   ├─> Delta decomposition (UPDATE → DELETE + INSERT)
   └─> Commit coordination (RowDelta or OverwriteFiles)

3. Writer coordination:
   ├─> DeltaWriter with dual sub-writers
   │     (data writer + delete writer)
   └─> Partition-aware routing
```

---

## 6. Row Identification and Metadata Columns

The Rust implementation has metadata column support that would be needed for UPDATE/MERGE:

```
Metadata columns defined in:                        [crates/iceberg/src/metadata_columns.rs]

| Column                          | Type    | Purpose                                 | Rust Status |
|---------------------------------|---------|-----------------------------------------|-------------|
| _file                           | string  | Data file path for row identification   | Defined     |
| _pos                            | long    | 0-based row position within file        | Defined     |
| _deleted                        | boolean | Delete marking for changelogs           | Defined     |
| _spec_id                        | int     | Partition spec version for routing      | Defined     |
| _partition                      | struct  | Partition values for routing            | Defined     |
| _change_type                    | string  | INSERT/DELETE/UPDATE_BEFORE/_AFTER tag  | Defined     |
| _change_ordinal                 | int     | Order of the change in a changelog      | Defined     |
| _commit_snapshot_id             | long    | Snapshot ID in which the change occurred| Defined     |
| _row_id                         | long    | Row lineage identity (V3)               | Defined     |
| _last_updated_sequence_number   | long    | Last modification tracking (V3)         | Defined     |
```

The field-ID constants, name constants, and lookup helpers
(`get_metadata_field`, `get_metadata_field_id`, `is_metadata_field`,
`is_metadata_column_name`) are all in place. Wiring these columns into a
row-level DML execution path (scan-side projection, write-side routing,
delta decomposition) is the missing piece, not the field definitions
themselves.

The Rust write path also already tracks V3 row lineage at commit time:
`SnapshotProducer` assigns `first_row_id` from `TableMetadata.next_row_id()`
and propagates per-manifest `first_row_id` in the manifest list (see
`crates/iceberg/src/transaction/snapshot.rs`). That means appends produced
by the Rust writer are V3-lineage-correct; the gap is on the DML side, not
in row-id assignment.

---

## 7. Isolation Levels and Conflict Detection

Java supports two isolation levels for UPDATE/MERGE conflict detection. The Rust implementation does not yet have this:

```
┌────────────────┬───────────────────────────────────────────────────────┐
│  SERIALIZABLE  │ Validates no concurrent data OR delete changes        │
│  (default)     │ in affected partitions. Retries on conflict.          │
│                │ Prevents phantom reads.                               │
├────────────────┼───────────────────────────────────────────────────────┤
│  SNAPSHOT      │ Only validates no concurrent delete changes.          │
│                │ Allows concurrent inserts into same partition.        │
│                │ Higher throughput, acceptable phantom reads.          │
└────────────────┴───────────────────────────────────────────────────────┘

Concurrent Operation Scenarios (Java behavior):

┌───────────────────────────────┬──────────────┬──────────────┐
│  Concurrent Operation         │ SERIALIZABLE │ SNAPSHOT     │
├───────────────────────────────┼──────────────┼──────────────┤
│ INSERT into same partition    │ FAILS        │ SUCCEEDS     │
│ INSERT into other partition   │ SUCCEEDS     │ SUCCEEDS     │
│ DELETE on same partition      │ FAILS        │ FAILS        │
│ UPDATE on same partition      │ FAILS        │ FAILS        │
│ MERGE on different partitions │ SUCCEEDS     │ SUCCEEDS     │
│ Compaction on affected files  │ FAILS        │ FAILS        │
└───────────────────────────────┴──────────────┴──────────────┘

FAILS = commit rejected, retried with exponential backoff
SUCCEEDS = commits proceed without conflict

Rust status: PARTIAL
  Optimistic commit conflict detection IS in place:
    - Snapshot-producing actions emit
      `TableRequirement::UuidMatch` and
      `TableRequirement::RefSnapshotIdMatch`
      (see crates/iceberg/src/transaction/snapshot.rs).
    - The catalog enforces these as retryable
      `CatalogCommitConflicts` errors
      (see crates/iceberg/src/catalog/mod.rs), and
      Transaction.commit() retries with exponential backoff
      (via `backon`).
  What is NOT yet implemented:
    - Row-level UPDATE/MERGE conflict validation
      (e.g., validate scanned data/delete files, validate
      no conflicting partition data/deletes).
    - Explicit `SERIALIZABLE` vs `SNAPSHOT` isolation modes
      and the per-isolation behavior table above.
```

---

## 8. Java vs Rust Component Mapping

| Java Component                         | Purpose                             | Rust Equivalent                        | Status          |
|----------------------------------------|-------------------------------------|----------------------------------------|-----------------|
| `SparkCopyOnWriteOperation`            | CoW entry point                     | (none)                                 | Not implemented |
| `SparkPositionDeltaOperation`          | MoR entry point                     | (none)                                 | Not implemented |
| `SparkCopyOnWriteScan`                 | CoW scan with metadata columns      | (none)                                 | Not implemented |
| `OverwriteFiles` (API)                 | CoW commit API                      | (none)                                 | Not implemented |
| `RowDelta` (API)                       | MoR commit API                      | (none)                                 | Not implemented |
| `BaseRowDelta`                         | MoR commit implementation           | (none)                                 | Not implemented |
| `SparkPositionDeltaWrite`              | MoR write orchestration             | (none)                                 | Not implemented |
| `UnpartitionedDeltaWriter`             | MoR unpartitioned writer            | (none)                                 | Not implemented |
| `PartitionedDeltaWriter`               | MoR partitioned writer              | (none)                                 | Not implemented |
| `DeleteOnlyDeltaWriter`                | MoR delete-only writer              | (none)                                 | Not implemented |
| `ClusteredPositionDeleteWriter`        | V2 ordered position deletes         | (none)                                 | Not implemented |
| `FanoutPositionOnlyDeleteWriter`       | V2 unordered position deletes       | (none)                                 | Not implemented |
| `PartitioningDVWriter`                 | V3 deletion vector writer           | (none)                                 | Not implemented |
| `SparkRowLevelOperationBuilder`        | CoW vs MoR mode dispatch            | (none)                                 | Not implemented |
| `IsolationLevel`                       | SERIALIZABLE / SNAPSHOT             | (none)                                 | Not implemented |
| `Transaction`                          | Transaction with actions            | `Transaction`                          | Exists          |
| `TransactionAction` (trait)            | Pluggable actions                   | `TransactionAction` (trait)            | Exists          |
| `FastAppendAction`                     | Append data files                   | `FastAppendAction`                     | Exists          |
| `UpdateSchemaAction` (add/delete col)  | Schema evolution                    | `UpdateSchemaAction`                   | Exists (partial — add + delete) |
| `SnapshotProducer`                     | Snapshot creation                   | `SnapshotProducer`                     | Exists          |
| `DeleteFileIndex`                      | Delete file lookup                  | `DeleteFileIndex`                      | Exists          |
| `DeleteFilter`                         | Delete application during reads     | `DeleteFilter`                         | Exists          |
| `PositionDeleteIndex` (bitmap)         | Position delete bitmap              | `DeleteVector` (RoaringTreemap)        | Exists          |
| `EqualityDeleteWriter`                 | Equality delete file writing        | `EqualityDeleteFileWriter`             | Exists          |
| `MetadataColumns`                      | _file, _pos, _spec_id, etc.         | `metadata_columns`                     | Exists (definitions only — not yet projected/routed in DML) |
| `FanoutPositionOnlyWriter` (data side) | Unsorted partition fan-out          | `FanoutWriter`                         | Exists (data only) |
| `ClusteredDataWriter`                  | Sorted partition writer             | `ClusteredWriter`                      | Exists (data only) |
| `UnpartitionedWriter` (data side)      | Unpartitioned table writer          | `UnpartitionedWriter`                  | Exists (data only) |
| `TableProperties` (mode config)        | write.update.mode, etc.             | (not used for mode dispatch)           | Not applicable  |

---

## 9. Configuration (Java Reference)

These table properties control UPDATE/MERGE behavior in Java. They would need Rust equivalents when row-level DML is implemented:

| Property                       | Default              | Values                           | Scope               |
|--------------------------------|----------------------|----------------------------------|---------------------|
| `write.update.mode`            | `copy-on-write`      | `copy-on-write`, `merge-on-read` | UPDATE operations   |
| `write.merge.mode`             | `copy-on-write`      | `copy-on-write`, `merge-on-read` | MERGE operations    |
| `write.delete.mode`            | `copy-on-write`      | `copy-on-write`, `merge-on-read` | DELETE operations   |
| `write.update.isolation-level` | `serializable`       | `serializable`, `snapshot`       | UPDATE conflicts    |
| `write.merge.isolation-level`  | `serializable`       | `serializable`, `snapshot`       | MERGE conflicts     |
| `write.distribution-mode`      | `hash`               | `none`, `hash`, `range`          | Shuffle strategy    |
| `write.target-file-size-bytes` | `536870912` (512 MB) | long                             | Target file size    |
