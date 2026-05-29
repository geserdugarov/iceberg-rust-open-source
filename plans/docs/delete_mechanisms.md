# Iceberg Rust Delete Mechanisms — Position Deletes, Equality Deletes, and Deletion Vectors

This document describes how Apache Iceberg Rust handles row-level deletes, covering the read-side implementation of Position Deletes and Equality Deletes, plus the current state of Deletion Vector (Puffin) support (read-side not yet wired up).

**Related docs:** [architecture_overview.md](architecture_overview.md) | [read_path_callstack.md](read_path_callstack.md) | [write_path_callstack.md](write_path_callstack.md) | [update_and_merge_mechanisms.md](update_and_merge_mechanisms.md)

---

## 1. Overview — The Iceberg Delete Model

Iceberg data files are **immutable** — once written, they are never modified in place. To support SQL operations that logically remove or update rows (DELETE, UPDATE, MERGE INTO), Iceberg uses two strategies:

- **Copy-on-Write (CoW):** Rewrite entire data files with the affected rows removed. No delete files are produced.
- **Merge-on-Read (MoR):** Write lightweight **delete files** that record which rows are logically deleted. The deletes are applied at read time.

The Rust implementation currently focuses on the **read-side** of MoR. It can correctly apply Parquet position deletes and equality deletes during table scans. Puffin-format deletion vectors are not yet read end-to-end: the in-memory `DeleteVector` (RoaringTreemap) is built from position-delete Parquet files, and the Puffin reader/`DELETION_VECTOR_V1` blob type are in place, but the delete-file loader does not yet decode Puffin DV blobs (see `TODO: Delete Vector loader from Puffin files` in `caching_delete_file_loader.rs` and `TODO: Deletion Vector support` in `delete_file_index.rs`).

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    ICEBERG DELETE FILE TYPES                               │
│                                                                            │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌────────────────────┐  │
│  │  Position Deletes   │  │  Equality Deletes   │  │ Deletion Vectors   │  │
│  │  (V2+)              │  │  (V2+)              │  │ (V3+)              │  │
│  │                     │  │                     │  │                    │  │
│  │  Parquet file with  │  │  Parquet file with  │  │  RoaringTreemap    │  │
│  │  file_path + pos    │  │  data rows matching │  │  bitmap per data   │  │
│  │  columns            │  │  equality fields    │  │  file              │  │
│  │                     │  │                     │  │                    │  │
│  │  Scope: per file    │  │  Scope: partition   │  │  Scope: per file   │  │
│  │  Apply: O(1) bitmap │  │  Apply: predicate   │  │  Apply: O(1) bitmap│  │
│  │  → RowSelection     │  │  via Arrow row      │  │  → RowSelection    │  │
│  │                     │  │  filter             │  │                    │  │
│  └─────────────────────┘  └─────────────────────┘  └────────────────────┘  │
│                                                                            │
│  Rust read support:  YES            YES               NO (TODO; Puffin     │
│                                                          DV loader missing)│
│  Rust write support: NO             YES               NO                   │
│                                                                            │
│  Format Version:  V1 = no deletes (append-only)                            │
│                   V2 = position deletes + equality deletes                 │
│                   V3 = adds deletion vectors (Puffin format)               │
└────────────────────────────────────────────────────────────────────────────┘
```

### Comparison Table

| Property                    | Position Deletes                       | Equality Deletes                                | Deletion Vectors                           |
|-----------------------------|----------------------------------------|-------------------------------------------------|--------------------------------------------|
| **Format version**          | V2+                                    | V2+                                             | V3+                                        |
| **File format**             | Parquet                                | Parquet                                         | Puffin (binary blob)                       |
| **Content type**            | `POSITION_DELETES`                     | `EQUALITY_DELETES`                              | `POSITION_DELETES`                         |
| **Scope**                   | Per-row, by the `file_path` column (one delete file can address rows in multiple data files) | All files in partition matching equality fields | Single data file (by `referencedDataFile`) |
| **Columns stored**          | `file_path` (string) + `pos` (long)    | All equality field columns                      | Serialized RoaringBitmap of positions      |
| **Read-time cost**          | Low — bitmap lookup O(1) per row       | Higher — predicate evaluated per row via Arrow row filter | Lowest — compact bitmap, direct access     |
| **Write-time cost**         | Low — record (path, pos) pairs         | Low — record matching rows                      | Low — set bits in bitmap                   |
| **Storage overhead**        | Moderate — one record per deleted row  | Moderate — equality-key column values per deleted row | Low — compressed bitmap                    |
| **Multiple per data file?** | Yes (across multiple delete files)     | N/A (partition-scoped)                          | No (at most one DV per data file)          |
| **Rust read support**       | Yes                                    | Yes                                             | No (TODO; Puffin DV blob loader missing)   |
| **Rust write support**      | No                                     | Yes (EqualityDeleteFileWriter)                  | No                                         |

---

## 2. Delete File Index

The `DeleteFileIndex` is the central structure that associates delete files with data files during scan planning.

### Structure

```
DeleteFileIndex                                     [crates/iceberg/src/delete_file_index.rs]
  │
  └─> state: Arc<RwLock<DeleteFileIndexState>>
        │
        ├── Populating(Arc<Notify>)          being loaded asynchronously
        └── Populated(PopulatedDeleteFileIndex)

PopulatedDeleteFileIndex
  │
  ├── global_equality_deletes: Vec<Arc<DeleteFileContext>>
  │     Equality deletes from manifests with an empty partition spec; applied
  │     globally per Iceberg spec.
  │
  ├── eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>
  │     Equality deletes indexed by partition values
  │
  └── pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>
        Position deletes indexed by partition values

  TODO in source: Deletion-vector indexing is not yet implemented.
```

### Population Flow

```
TableScan.plan_files()
  │
  ├─> (delete_file_index, delete_file_tx) = DeleteFileIndex::new()
  │     └─> Spawns async task with channel receiver
  │
  ├─> For each delete manifest in snapshot:
  │     spawn task:
  │       read manifest entries
  │       for each entry with content == Deletes:
  │         delete_file_tx.send(DeleteFileContext {
  │           manifest_entry,
  │           partition_spec_id,
  │         })
  │
  ├─> drop(delete_file_tx)  (signals completion)
  │
  └─> Async task collects all DeleteFileContext entries:
        PopulatedDeleteFileIndex::new(delete_files)
          ├─> Partition deletes by type:
          │     DataContentType::PositionDeletes → pos_deletes_by_partition
          │     DataContentType::EqualityDeletes → eq_deletes_by_partition
          │     (global equality deletes stored separately)
          └─> Set state to Populated, notify waiters
```

### Lookup: get_deletes_for_data_file()

```
DeleteFileIndex.get_deletes_for_data_file(data_file, seq_num)
  │
  ├─> Wait for population (if still Populating):
  │     notifier.notified().await
  │
  └─> PopulatedDeleteFileIndex.get_deletes_for_data_file():
        │
        ├─> Collect global equality deletes (no partition predicate)
        │
        ├─> Collect equality deletes for this partition:
        │     eq_deletes_by_partition.get(data_file.partition)
        │     also enforces data_file.partition_spec_id == delete.partition_spec_id
        │
        ├─> Collect position deletes for this partition:
        │     pos_deletes_by_partition.get(data_file.partition)
        │     also enforces data_file.partition_spec_id == delete.partition_spec_id
        │
        ├─> Filter by sequence number (per Iceberg spec):
        │     - equality deletes (global + partitioned):
        │         delete.sequence_number > data.sequence_number
        │     - position deletes:
        │         delete.sequence_number >= data.sequence_number
        │     (a None seq_num means "include all")
        │
        └─> Return Vec<FileScanTaskDeleteFile>
```

> Note: the source still has a `TODO` that the position-delete filter does not
> yet honour the optional `referenced_data_file` field on a delete file. Today
> all position-delete files for a matching partition are returned and the
> per-row `file_path` column inside each delete file is what ultimately
> narrows the match in the Arrow reader.

---

## 3. Delete Vector (Position Delete Bitmap)

The `DeleteVector` efficiently stores deleted row positions using a `RoaringTreemap` (64-bit Roaring bitmap).

```
DeleteVector                                        [crates/iceberg/src/delete_vector.rs]
  │
  └─> inner: RoaringTreemap
        │
        ├─> insert(pos: u64) → bool
        │     Mark a single position as deleted
        │
        ├─> insert_positions(positions: &[u64]) → Result<usize>
        │     Bulk insert (must be strictly ascending order)
        │
        ├─> iter() → DeleteVectorIterator
        │     Iterate over all deleted positions
        │     (supports advance_to() for efficient seeking)
        │
        └─> len() → u64
              Number of deleted positions

DeleteVectorIterator<'a>
  │
  └─> Custom iterator over RoaringTreemap
      Wraps BitmapIter for 64-bit position support
      Outputs u64 positions in ascending order

  Used in ArrowReader to build RowSelection:
    for each row group:
      advance_to(row_group_start)
      collect positions within [start, start+num_rows)
      create RowSelection that skips deleted rows
```

---

## 4. Delete Filter

The `DeleteFilter` manages the application of delete files during Arrow record batch reading.

### Structure

```
DeleteFilter                                        [crates/iceberg/src/arrow/delete_filter.rs]
  │
  ├── state: Arc<RwLock<DeleteFileFilterState>>
  └── runtime: Runtime

DeleteFileFilterState
  │
  ├── delete_vectors: HashMap<String, Arc<Mutex<DeleteVector>>>
  │     data_file_path → position delete bitmap
  │     Loaded from position delete files (DV blobs not yet supported)
  │
  ├── equality_deletes: HashMap<String, EqDelState>
  │     delete_file_path → loading state
  │
  └── positional_deletes: HashMap<String, PosDelState>
        delete_file_path → loading state

EqDelState (enum)
  ├── Loading(Arc<Notify>)     being loaded asynchronously
  └── Loaded(Predicate)        ready to evaluate

PosDelState (enum)
  ├── Loading(Arc<Notify>)     being loaded asynchronously
  └── Loaded                   fully loaded into delete_vectors

PosDelLoadAction (enum, returned from try_start_pos_del_load)
  ├── Load                     caller should load this file
  ├── AlreadyLoaded            no-op
  └── WaitFor(Arc<Notify>)     another task is loading; caller must await
```

### Key Methods

```
DeleteFilter.get_delete_vector(&FileScanTask)
  └─> Returns Option<Arc<Mutex<DeleteVector>>>
      The bitmap of deleted positions for this data file
      (also: get_delete_vector_for_path(&str) variant)

DeleteFilter.try_start_eq_del_load(delete_file_path)
  └─> Returns Option<Arc<Notify>>
      Some(notifier) if the caller is the first to register the file —
        the caller must perform the load and eventually publish a Predicate.
      None if another task has already started/finished loading it.

DeleteFilter.try_start_pos_del_load(delete_file_path) -> PosDelLoadAction
  └─> Coordinates concurrent loading of positional delete files.
      Returns Load / AlreadyLoaded / WaitFor(Notify).

DeleteFilter.finish_pos_del_load(delete_file_path)
  └─> Transitions PosDelState from Loading to Loaded and notifies waiters
      once the loader has merged all bitmaps into delete_vectors.

DeleteFilter.upsert_delete_vector(data_file_path, DeleteVector)
  └─> Merges (bitor) a freshly parsed DeleteVector into the per-data-file
      bitmap, creating the entry if absent.

DeleteFilter.insert_equality_delete(delete_file_path, oneshot::Receiver<Predicate>)
  └─> Registers an in-flight equality-delete load whose Predicate will
      arrive via the oneshot channel; spawns a runtime task that flips
      the entry to Loaded(Predicate) and notifies waiters when the
      Predicate arrives.

DeleteFilter.build_equality_delete_predicate(&FileScanTask) -> Result<Option<BoundPredicate>>
  └─> Combines all equality-delete predicates referenced by the task with
      logical AND and binds the result to the task's schema.
```

---

## 5. Delete File Loader

### Trait

```
DeleteFileLoader                                    [crates/iceberg/src/arrow/delete_file_loader.rs]
  │
  └─> async fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef
      ) → Result<ArrowRecordBatchStream>
```

### Implementation

```
BasicDeleteFileLoader                               [crates/iceberg/src/arrow/delete_file_loader.rs]
  │
  ├─> file_io:      FileIO
  ├─> scan_metrics: ScanMetrics
  │
  ├─> parquet_to_batch_stream(data_file_path, file_size_in_bytes)
  │     Open Parquet via FileIO + ArrowReader::open_parquet_file
  │     Build ParquetRecordBatchStream and return it as ArrowRecordBatchStream
  │
  ├─> evolve_schema(stream, target_schema, equality_ids)  (associated fn)
  │     Project the stream's RecordBatches with
  │     RecordBatchTransformerBuilder::new(target_schema, equality_ids)
  │
  └─> read_delete_file(task, schema)  (trait impl)
        Opens the Parquet stream, then for equality deletes evolves
        the schema using task.equality_ids; for positional deletes
        evolves using every field id in the schema.

CachingDeleteFileLoader                             [crates/iceberg/src/arrow/caching_delete_file_loader.rs]
  │
  ├─> basic_delete_file_loader:    BasicDeleteFileLoader
  ├─> concurrency_limit_data_files: usize
  ├─> delete_filter:               DeleteFilter   (the shared cache)
  ├─> runtime:                     Runtime
  │
  └─> load_deletes(delete_file_entries, schema) -> oneshot::Receiver<Result<DeleteFilter>>
        Drives loading of every applicable delete file for a batch of
        scan tasks, deduplicating across data files via DeleteFilter
        state. For each task:
          PositionDeletes: open Parquet stream and parse into
            HashMap<data_file_path, DeleteVector>, then upsert into
            DeleteFilter.delete_vectors and finish_pos_del_load().
          EqualityDeletes: insert a placeholder predicate slot, spawn
            load + parse into an unbound Predicate, send it through a
            oneshot channel into the DeleteFilter.
          (Puffin DV blob loading is documented in the source as a TODO.)
```

---

## 6. Equality Delete Writer

The Rust implementation includes a writer for equality delete files:

```
EqualityDeleteFileWriterBuilder<B, L, F>            [crates/iceberg/src/writer/base_writer/equality_delete_writer.rs]
  │
  ├── inner: RollingFileWriterBuilder<B, L, F>
  └── config: EqualityDeleteWriterConfig

EqualityDeleteWriterConfig
  │
  ├── equality_ids: Vec<i32>
  │     Field IDs that determine equality matching
  │
  ├── projector: RecordBatchProjector
  │     Projects input batches to equality columns only
  │
  └── new(equality_ids, schema) → Result<Self>:
        Per the Iceberg spec, equality-delete columns cannot be float or
        double. The current Rust validator (via RecordBatchProjector) is:
          ├─> Reject equality_ids pointing at a top-level Struct, List, or
          │   Map field, or at any field nested inside a List/Map.
          ├─> Reject equality_ids pointing at Float16 / Float32 / Float64
          │   (whether top-level or nested inside a Struct).
          ├─> Accept primitive fields nested inside a Struct.
          └─> Accept nullable (optional) fields — see
              `test_equality_delete_with_nullable_field`.

EqualityDeleteFileWriter<B, L, F>
  │
  ├─> write(RecordBatch):
  │     projector.project(batch) → projected_batch
  │     rolling_writer.write(projected_batch)
  │
  └─> close() → Vec<DataFile>:
        rolling_writer.close()
        for each file:
          set content_type = EqualityDeletes
          set equality_ids
          set partition info
```

---

## 7. Read-Time Delete Application — Unified Flow

### Planning Phase

```
TableScan.plan_files()
  │
  └─> For each data manifest entry:
        │
        ├─> Apply partition + metrics filters (skip non-matching files)
        │
        └─> ManifestEntryContext.into_file_scan_task()
              │
              ├─> delete_file_index.get_deletes_for_data_file(data_file, seq_num)
              │     │
              │     ├─> Sequence number filtering (see § 2 for full rules):
              │     │     equality deletes:  delete.seq > data.seq
              │     │     position deletes:  delete.seq >= data.seq
              │     │
              │     ├─> For position deletes: match by partition + partition_spec_id
              │     ├─> For equality deletes: match by partition + partition_spec_id,
              │     │   plus global (unpartitioned) equality deletes
              │     └─> Return Vec<FileScanTaskDeleteFile>
              │
              └─> FileScanTask {
                    data_file_path,
                    schema,
                    predicate,
                    deletes: Vec<FileScanTaskDeleteFile>,  <── associated delete files
                    ...
                  }
```

### Sequence Number Rule

Per the Iceberg spec, equality deletes apply when `delete.seq > data.seq`,
while position deletes apply when `delete.seq >= data.seq` (a position delete
written in the same snapshot as its data file is still valid).

```
Timeline (equality-delete example):

  Snapshot 1 (seq=1):  data-file-A added
  Snapshot 2 (seq=2):  data-file-B added
  Snapshot 3 (seq=3):  eq-delete-file-X added
  Snapshot 4 (seq=4):  data-file-C added

  eq-delete-file-X.sequence_number = 3

  Applies to data-file-A?  YES  (3 > 1)
  Applies to data-file-B?  YES  (3 > 2)
  Applies to data-file-C?  NO   (3 < 4)  <── data added AFTER the delete

  For a position delete with sequence_number = 3, it would also apply to a
  data file in the same snapshot (data.seq = 3) because the comparison is
  `>=` for position deletes.

  This prevents retroactive application of deletes to data written later.
```

### Execution Phase (Arrow Reader)

The per-task pipeline lives in `FileScanTaskReader::process` inside
`crates/iceberg/src/arrow/reader/pipeline.rs` and is driven by `ArrowReader::read`.
The `ArrowReader` struct itself is declared in
`crates/iceberg/src/arrow/reader/mod.rs` and aggregates the
`CachingDeleteFileLoader`, file IO, concurrency limit, and Parquet read
options.

```
FileScanTaskReader::process(task)                   [crates/iceberg/src/arrow/reader/pipeline.rs]
  │
  ├─> Kick off delete loading in parallel:
  │     delete_filter_rx = CachingDeleteFileLoader
  │       .load_deletes(&task.deletes, task.schema)
  │
  ├─> Open the data Parquet file:
  │     ArrowReader::open_parquet_file(...)
  │     Resolve / assign field IDs (embedded, name mapping, or
  │     position fallback), then coerce INT96 timestamps.
  │
  ├─> Build the ParquetRecordBatchStream builder with projection mask,
  │     RecordBatchTransformer (partition constants, _file column, ...),
  │     and optional batch size.
  │
  ├─> Await delete_filter_rx → DeleteFilter
  │     delete_predicate = delete_filter.build_equality_delete_predicate(&task)
  │
  ├─> Combine task.predicate AND delete_predicate (if any) and feed to
  │     RecordBatchStreamBuilder.with_row_filter / row-group filtering /
  │     row selection (depending on enabled options).
  │
  ├─> Positional deletes:
  │     positional_delete_indexes = delete_filter.get_delete_vector(&task)
  │     ArrowReader::build_deletes_row_selection(
  │         row_group_metadata, selected_row_groups,
  │         &*positional_delete_indexes)
  │       → RowSelection skipping every position in the bitmap
  │     Merge with any predicate-derived row selection via intersection.
  │
  └─> Return the resulting ArrowRecordBatchStream
```

`build_deletes_row_selection` is defined on `ArrowReader` in
`crates/iceberg/src/arrow/reader/positional_deletes.rs`. Equality deletes are
applied via the combined Arrow row filter described above (not as a
separate post-processing step over `RecordBatch`es).

---

## 8. SQL Operation → Delete Type Matrix

```
┌───────────────┬────────────────────────┬─────────────────────────────────────────┐
│  SQL Command  │  Copy-on-Write (CoW)   │  Merge-on-Read (MoR)                    │
│               │                        │                                         │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  DELETE       │  NOT IMPLEMENTED       │  Read-side: IMPLEMENTED                 │
│  FROM ...     │  (no OverwriteFiles)   │  (reads position deletes & equality     │
│  WHERE ...    │                        │   deletes; Puffin DVs NOT yet)          │
│               │                        │  Write-side: NOT IMPLEMENTED — no SQL   │
│               │                        │  DELETE / commit path (no RowDelta);    │
│               │                        │  only the low-level                     │
│               │                        │  EqualityDeleteFileWriter exists for    │
│               │                        │  producing equality-delete data files.  │
│               │                        │                                         │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  UPDATE       │  NOT IMPLEMENTED       │  NOT IMPLEMENTED                        │
│  ...          │                        │  (no RowDelta action)                   │
│  SET ...      │                        │                                         │
│               │                        │                                         │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  MERGE INTO   │  NOT IMPLEMENTED       │  NOT IMPLEMENTED                        │
│               │                        │  (no RowDelta action)                   │
│               │                        │                                         │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  INSERT INTO  │  IMPLEMENTED           │  IMPLEMENTED                            │
│               │  (FastAppendAction)    │  (same — append only)                   │
│               │                        │                                         │
└───────────────┴────────────────────────┴─────────────────────────────────────────┘
```

---

## 9. What's NOT Implemented (vs Java)

| Feature                        | Java Status              | Rust Status                    |
|--------------------------------|--------------------------|--------------------------------|
| Position delete file writing   | Yes (Parquet)            | No                             |
| Equality delete file writing   | Yes (Parquet)            | Yes (EqualityDeleteFileWriter) |
| Deletion vector writing        | Yes (Puffin)             | No                             |
| Position delete reading        | Yes                      | Yes                            |
| Equality delete reading        | Yes                      | Yes                            |
| Deletion vector reading        | Yes (Puffin blob)        | No (TODO; Puffin DV blob loader missing) |
| RowDelta commit API            | Yes                      | No                             |
| OverwriteFiles commit API      | Yes                      | No                             |
| CoW DELETE execution           | Yes (via Spark)          | No                             |
| MoR DELETE execution           | Yes (via Spark)          | No (read-side only)            |
| CoW UPDATE execution           | Yes (via Spark)          | No                             |
| MoR UPDATE execution           | Yes (via Spark)          | No                             |
| Delete file marking mode       | Yes (`_deleted` column)  | No                             |
| Schema expansion for eq deletes| Yes (auto-add columns)   | Partial                        |
| Delete file compaction         | Yes (RewriteDataFiles)   | No                             |

---

## 10. Key Structs Reference

| Area                        | Struct/Trait                   | File                                                    | Key Method                                   |
|-----------------------------|-------------------------------|----------------------------------------------------------|----------------------------------------------|
| **Delete index**            | `DeleteFileIndex`             | crates/iceberg/src/delete_file_index.rs                         | `new()`, `get_deletes_for_data_file()`       |
|                             | `PopulatedDeleteFileIndex`    | crates/iceberg/src/delete_file_index.rs                         | partition-indexed lookup                     |
| **Delete vector**           | `DeleteVector`                | crates/iceberg/src/delete_vector.rs                             | `insert()`, `iter()`, `len()`                |
|                             | `DeleteVectorIterator`        | crates/iceberg/src/delete_vector.rs                             | `advance_to()`, `next()`                     |
| **Delete filter**           | `DeleteFilter`                | crates/iceberg/src/arrow/delete_filter.rs                       | `get_delete_vector()`, `try_start_pos_del_load()`, `try_start_eq_del_load()`, `build_equality_delete_predicate()` |
|                             | `DeleteFileFilterState`       | crates/iceberg/src/arrow/delete_filter.rs                       | state tracking for async loading             |
|                             | `PosDelLoadAction`            | crates/iceberg/src/arrow/delete_filter.rs                       | `Load` / `AlreadyLoaded` / `WaitFor`         |
| **Delete loader**           | `DeleteFileLoader` (trait)    | crates/iceberg/src/arrow/delete_file_loader.rs                  | `read_delete_file()`                         |
|                             | `BasicDeleteFileLoader`       | crates/iceberg/src/arrow/delete_file_loader.rs                  | `parquet_to_batch_stream()`, `evolve_schema()` |
|                             | `CachingDeleteFileLoader`     | crates/iceberg/src/arrow/caching_delete_file_loader.rs          | `load_deletes()`                             |
| **Eq delete writing**       | `EqualityDeleteFileWriter`    | crates/iceberg/src/writer/base_writer/equality_delete_writer.rs | `write()`, `close()`                         |
|                             | `EqualityDeleteFileWriterBuilder` | crates/iceberg/src/writer/base_writer/equality_delete_writer.rs | `build()` (impl of `IcebergWriterBuilder`) |
|                             | `EqualityDeleteWriterConfig`  | crates/iceberg/src/writer/base_writer/equality_delete_writer.rs | `new()` with validation                      |
| **Scan task**               | `FileScanTask`                | crates/iceberg/src/scan/task.rs                                 | `deletes: Vec<FileScanTaskDeleteFile>`       |
|                             | `FileScanTaskDeleteFile`      | crates/iceberg/src/scan/task.rs                                 | delete file reference in scan task           |
|                             | `DeleteFileContext`           | crates/iceberg/src/scan/task.rs                                 | manifest_entry + partition_spec_id           |
| **Data file types**         | `DataContentType`             | crates/iceberg/src/spec/manifest/data_file.rs                   | `Data`, `PositionDeletes`, `EqualityDeletes` (no `DeletionVector` variant) |
| **Arrow reader**            | `ArrowReader`                 | crates/iceberg/src/arrow/reader/mod.rs                          | aggregates loader, file IO, options          |
|                             | `FileScanTaskReader::process` | crates/iceberg/src/arrow/reader/pipeline.rs                     | per-task pipeline including delete handling  |
|                             | `ArrowReader::build_deletes_row_selection` | crates/iceberg/src/arrow/reader/positional_deletes.rs | `DeleteVector` → Parquet `RowSelection` |
| **Batch transform**         | `RecordBatchTransformer`      | crates/iceberg/src/arrow/record_batch_transformer.rs            | column projection for delete reads           |
| **Puffin (DV format)**      | `DELETION_VECTOR_V1`          | crates/iceberg/src/puffin/blob.rs                               | blob type constant for deletion-vector-v1    |
