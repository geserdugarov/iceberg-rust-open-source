# Iceberg Rust Delete Mechanisms — Position Deletes, Equality Deletes, and Deletion Vectors

This document provides a comprehensive description of how Apache Iceberg Rust handles row-level deletes, covering the read-side implementation of Position Deletes, Equality Deletes, and Deletion Vectors.

**Related docs:** [architecture_overview.md](architecture_overview.md) | [read_path_callstack.md](read_path_callstack.md) | [write_path_callstack.md](write_path_callstack.md) | [update_and_merge_mechanisms.md](update_and_merge_mechanisms.md)

---

## 1. Overview — The Iceberg Delete Model

Iceberg data files are **immutable** — once written, they are never modified in place. To support SQL operations that logically remove or update rows (DELETE, UPDATE, MERGE INTO), Iceberg uses two strategies:

- **Copy-on-Write (CoW):** Rewrite entire data files with the affected rows removed. No delete files are produced.
- **Merge-on-Read (MoR):** Write lightweight **delete files** that record which rows are logically deleted. The deletes are applied at read time.

The Rust implementation currently focuses on the **read-side** of MoR — it can correctly apply all three types of delete files during table scans.

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
│  │  Apply: O(1) bitmap │  │  Apply: hash lookup │  │  Apply: O(1) bitmap│  │
│  └─────────────────────┘  └─────────────────────┘  └────────────────────┘  │
│                                                                            │
│  Rust read support:  YES            YES               YES (via DeleteVector)│
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
| **Scope**                   | Single data file (by `file_path`)      | All files in partition matching equality fields | Single data file (by `referencedDataFile`) |
| **Columns stored**          | `file_path` (string) + `pos` (long)    | All equality field columns                      | Serialized RoaringBitmap of positions      |
| **Read-time cost**          | Low — bitmap lookup O(1) per row       | High — hash set lookup per row                  | Lowest — compact bitmap, direct access     |
| **Write-time cost**         | Low — record (path, pos) pairs         | Low — record matching rows                      | Low — set bits in bitmap                   |
| **Storage overhead**        | Moderate — one record per deleted row  | High — full row per deleted row                 | Low — compressed bitmap                    |
| **Multiple per data file?** | Yes (across multiple delete files)     | N/A (partition-scoped)                          | No (at most one DV per data file)          |
| **Rust read support**       | Yes                                    | Yes                                             | Yes (via DeleteVector)                     |
| **Rust write support**      | No                                     | Yes (EqualityDeleteFileWriter)                  | No                                         |

---

## 2. Delete File Index

The `DeleteFileIndex` is the central structure that associates delete files with data files during scan planning.

### Structure

```
DeleteFileIndex                                     [iceberg/src/delete_file_index.rs]
  │
  └─> state: Arc<RwLock<DeleteFileIndexState>>
        │
        ├── Populating(Arc<Notify>)          being loaded asynchronously
        └── Populated(PopulatedDeleteFileIndex)

PopulatedDeleteFileIndex
  │
  ├── global_equality_deletes: Vec<Arc<DeleteFileContext>>
  │     Equality deletes that apply to all partitions
  │
  ├── eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>
  │     Equality deletes indexed by partition values
  │
  └── pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>
        Position deletes indexed by partition values
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
        ├─> Collect position deletes for this partition:
        │     pos_deletes_by_partition.get(data_file.partition)
        │
        ├─> Collect equality deletes for this partition:
        │     eq_deletes_by_partition.get(data_file.partition)
        │     + global_equality_deletes
        │
        ├─> Filter by sequence number:
        │     Only include deletes where:
        │       delete.sequence_number > data_file.sequence_number
        │     (prevents retroactive application of deletes to newer data)
        │
        └─> Return Vec<FileScanTaskDeleteFile>
```

---

## 3. Delete Vector (Position Delete Bitmap)

The `DeleteVector` efficiently stores deleted row positions using a `RoaringTreemap` (64-bit Roaring bitmap).

```
DeleteVector                                        [iceberg/src/delete_vector.rs]
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
DeleteFilter                                        [iceberg/src/arrow/delete_filter.rs]
  │
  └─> state: Arc<RwLock<DeleteFileFilterState>>

DeleteFileFilterState
  │
  ├── delete_vectors: HashMap<String, Arc<Mutex<DeleteVector>>>
  │     file_path → position delete bitmap
  │     Loaded from position delete files and DVs
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
```

### Key Methods

```
DeleteFilter.get_delete_vector(data_file_path)
  └─> Returns Option<Arc<Mutex<DeleteVector>>>
      The bitmap of deleted positions for this data file

DeleteFilter.try_start_eq_del_load(delete_file_path)
  └─> Coordinates concurrent loading of equality deletes
      Returns true if this caller should perform the load
      Other callers wait on the Notify
```

---

## 5. Delete File Loader

### Trait

```
DeleteFileLoader                                    [iceberg/src/arrow/delete_file_loader.rs]
  │
  └─> async fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef
      ) → Result<ArrowRecordBatchStream>
```

### Implementation

```
BasicDeleteFileLoader                               [iceberg/src/arrow/delete_file_loader.rs]
  │
  ├─> file_io: FileIO
  │
  ├─> read_delete_file(task, schema):
  │     ├─> parquet_to_batch_stream(task)
  │     │     Open Parquet file via file_io
  │     │     Build ParquetRecordBatchStream
  │     │     Return Arrow stream
  │     │
  │     └─> For equality deletes:
  │           evolve_schema(task, schema)
  │             Project to equality field IDs only
  │             Use RecordBatchTransformerBuilder
  │
  └─> Schema evolution:
        For equality deletes: evolve only equality_ids columns
        For position deletes: use standard delete schema (file_path, pos)

CachingDeleteFileLoader                             [iceberg/src/arrow/caching_delete_file_loader.rs]
  │
  └─> Wraps BasicDeleteFileLoader with caching
      Avoids re-reading the same delete file for multiple data files
```

---

## 6. Equality Delete Writer

The Rust implementation includes a writer for equality delete files:

```
EqualityDeleteFileWriterBuilder<B, L, F>            [iceberg/src/writer/base_writer/equality_delete_writer.rs]
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
        Validates:
          ├─> Only primitive types allowed for equality fields
          ├─> Floating point types rejected
          └─> Nullable fields rejected

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
              │     ├─> Sequence number filtering:
              │     │     include delete only if
              │     │     delete.sequence_number > data.sequence_number
              │     │
              │     ├─> For position deletes: match by partition
              │     ├─> For equality deletes: match by partition + field scope
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

```
Timeline:

  Snapshot 1 (seq=1):  data-file-A added
  Snapshot 2 (seq=2):  data-file-B added
  Snapshot 3 (seq=3):  delete-file-X added (deletes from data-file-A)
  Snapshot 4 (seq=4):  data-file-C added

  delete-file-X.sequence_number = 3

  Applies to data-file-A?  YES  (3 > 1)
  Applies to data-file-B?  YES  (3 > 2)
  Applies to data-file-C?  NO   (3 < 4)  <── data added AFTER the delete

  This prevents retroactive application of deletes to newer data.
```

### Execution Phase (Arrow Reader)

```
ArrowReader.process_file_scan_task(task)            [iceberg/src/arrow/reader.rs]
  │
  │  DELETE APPLICATION ORDER:
  │  Position deletes first, then equality deletes
  │
  ├─> Step 1: Load position deletes
  │     CachingDeleteFileLoader
  │       .load_position_deletes(pos_delete_files, data_file_path)
  │       │
  │       ├─> Read position delete Parquet files
  │       ├─> Filter records where file_path == current data file
  │       └─> Build DeleteVector (RoaringTreemap):
  │             delete_vector.insert(pos) for each deleted position
  │
  ├─> Step 2: Apply position deletes to row selection
  │     For each row group in Parquet file:
  │       delete_vector_iter.advance_to(row_group_start)
  │       Build RowSelection:
  │         row at pos N → if delete_vector.contains(N): skip
  │                        else: include
  │       Apply RowSelection to ParquetRecordBatchStream
  │
  ├─> Step 3: Load equality deletes
  │     CachingDeleteFileLoader
  │       .load_equality_deletes(eq_delete_files, schema)
  │       │
  │       ├─> Read equality delete Parquet files
  │       ├─> Project to equality field columns
  │       └─> Build predicate for filtering
  │
  ├─> Step 4: Apply equality deletes
  │     For each RecordBatch:
  │       Evaluate equality predicate against each row
  │       Filter out matching rows
  │
  └─> Return filtered ArrowRecordBatchStream
```

---

## 8. SQL Operation → Delete Type Matrix

```
┌───────────────┬────────────────────────┬─────────────────────────────────────────┐
│  SQL Command  │  Copy-on-Write (CoW)   │  Merge-on-Read (MoR)                    │
│               │                        │                                         │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  DELETE       │  NOT IMPLEMENTED       │  Read-side: IMPLEMENTED                 │
│  FROM ...     │  (no OverwriteFiles)   │  (can read position deletes,            │
│  WHERE ...    │                        │   equality deletes, and DVs)            │
│               │                        │  Write-side: equality deletes only      │
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
| Deletion vector reading        | Yes (Puffin blob)        | Yes (via DeleteVector)         |
| RowDelta commit API            | Yes                      | No                             |
| OverwriteFiles commit API      | Yes                      | No                             |
| CoW DELETE execution           | Yes (via Spark)          | No                             |
| MoR DELETE execution           | Yes (via Spark)          | No (read-side only)            |
| CoW UPDATE execution           | Yes (via Spark)          | No                             |
| MoR UPDATE execution           | Yes (via Spark)          | No                             |
| Delete file marking mode       | Yes (_is_deleted column) | No                             |
| Schema expansion for eq deletes| Yes (auto-add columns)   | Partial                        |
| Delete file compaction         | Yes (RewriteDataFiles)   | No                             |

---

## 10. Key Structs Reference

| Area                        | Struct/Trait                   | File                                                    | Key Method                                   |
|-----------------------------|-------------------------------|----------------------------------------------------------|----------------------------------------------|
| **Delete index**            | `DeleteFileIndex`             | iceberg/src/delete_file_index.rs                         | `new()`, `get_deletes_for_data_file()`       |
|                             | `PopulatedDeleteFileIndex`    | iceberg/src/delete_file_index.rs                         | partition-indexed lookup                     |
| **Delete vector**           | `DeleteVector`                | iceberg/src/delete_vector.rs                             | `insert()`, `iter()`, `len()`                |
|                             | `DeleteVectorIterator`        | iceberg/src/delete_vector.rs                             | `advance_to()`, `next()`                     |
| **Delete filter**           | `DeleteFilter`                | iceberg/src/arrow/delete_filter.rs                       | `get_delete_vector()`                        |
|                             | `DeleteFileFilterState`       | iceberg/src/arrow/delete_filter.rs                       | state tracking for async loading             |
| **Delete loader**           | `DeleteFileLoader` (trait)    | iceberg/src/arrow/delete_file_loader.rs                  | `read_delete_file()`                         |
|                             | `BasicDeleteFileLoader`       | iceberg/src/arrow/delete_file_loader.rs                  | Parquet reading + schema evolution           |
|                             | `CachingDeleteFileLoader`     | iceberg/src/arrow/caching_delete_file_loader.rs          | caching wrapper                              |
| **Eq delete writing**       | `EqualityDeleteFileWriter`    | iceberg/src/writer/base_writer/equality_delete_writer.rs | `write()`, `close()`                         |
|                             | `EqualityDeleteWriterConfig`  | iceberg/src/writer/base_writer/equality_delete_writer.rs | `new()` with validation                      |
| **Scan task**               | `FileScanTask`                | iceberg/src/scan/task.rs                                 | `deletes: Vec<FileScanTaskDeleteFile>`       |
|                             | `FileScanTaskDeleteFile`      | iceberg/src/scan/task.rs                                 | delete file reference in scan task           |
|                             | `DeleteFileContext`           | iceberg/src/scan/task.rs                                 | manifest_entry + partition_spec_id           |
| **Data file types**         | `DataContentType`             | iceberg/src/spec/manifest/data_file.rs                   | `Data`, `PositionDeletes`, `EqualityDeletes` |
| **Arrow reader**            | `ArrowReader`                 | iceberg/src/arrow/reader.rs                              | `process_file_scan_task()`                   |
| **Batch transform**         | `RecordBatchTransformer`      | iceberg/src/arrow/record_batch_transformer.rs            | column projection for delete reads           |
