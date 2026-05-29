# Iceberg Rust Compaction (RewriteDataFiles) — Call Stack

This document describes the state of compaction support in the Rust Iceberg implementation and how it compares to the Java implementation.

**Related docs:** [architecture_overview.md](architecture_overview.md) | [write_path_callstack.md](write_path_callstack.md) | [read_path_callstack.md](read_path_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md) | [update_and_merge_mechanisms.md](update_and_merge_mechanisms.md)

---

## 1. Current Status: NOT IMPLEMENTED

The Rust Iceberg implementation does **not yet have compaction (RewriteDataFiles) functionality**. There is no equivalent of the Java `RewriteDataFilesSparkAction`, `BinPackRewriteFilePlanner`, or `BaseRewriteFiles` operations.

Specifically, the following are missing:
- `RewriteFiles` transaction action (swap old files for new files)
- `OverwriteFiles` transaction action (replace files with filter)
- File planning / bin-packing algorithm
- Compaction strategy implementations (BinPack, Sort, ZOrder)
- Commit manager for partial progress

---

## 2. Infrastructure That Exists

While compaction itself is not implemented, the Rust codebase has foundational infrastructure that could be used to build it:

### 2.1 Transaction Action System

```
TransactionAction trait                             [iceberg/src/transaction/action.rs]
  │
  ├─> async fn commit(self: Arc<Self>, table: &Table) → Result<ActionCommit>
  │     ActionCommit {
  │       updates: Vec<TableUpdate>,
  │       requirements: Vec<TableRequirement>,
  │     }
  │
  ├─> Existing implementations:
  │     ├── FastAppendAction          (add data files)
  │     ├── UpdatePropertiesAction    (modify table properties)
  │     ├── UpgradeFormatVersionAction (V1→V2→V3)
  │     ├── UpdateStatisticsAction    (update stats files)
  │     ├── UpdateLocationAction      (change table location)
  │     ├── UpdateSchemaAction        (evolve table schema)
  │     └── ReplaceSortOrderAction    (change sort order)
  │
  └─> A "RewriteFilesAction" could be added following this pattern
      (the trait is currently `pub(crate)`, so adding a new action
       lives inside the `iceberg` crate)
```

### 2.2 SnapshotProducer

```
SnapshotProducer                                    [iceberg/src/transaction/snapshot.rs]
  │
  ├─> Today produces new snapshots that only:
  │     - Write an added-data manifest whose `ManifestEntry`
  │       rows are emitted with `ManifestStatus::Added` from
  │       `added_data_files`. `added_data_files` is populated
  │       via the (pub(crate)) constructor; the only public
  │       `add_data_files()` API is on `FastAppendAction`
  │       [iceberg/src/transaction/append.rs]
  │     - Carry forward existing `ManifestFile`s returned by
  │       `SnapshotProduceOperation::existing_manifest()` by
  │       appending them, by reference, to the new manifest
  │       list. The producer does NOT rewrite the contained
  │       `ManifestEntry` rows or coerce them to
  │       `ManifestStatus::Existing`; each entry keeps the
  │       status it was originally written with.
  │
  ├─> The SnapshotProduceOperation trait shape is:
  │       fn operation(&self) -> Operation
  │       fn delete_entries(&self, ...) -> Vec<ManifestEntry>
  │       fn existing_manifest(&self, ...) -> Vec<ManifestFile>
  │     but `delete_entries()` is NOT yet wired into the
  │     producer — `manifest_file()` has an explicit
  │     `# TODO Support process delete entries` and never
  │     emits a DELETED-status manifest today.
  │
  └─> Gaps before compaction can use SnapshotProducer:
        - Wire `delete_entries()` into `manifest_file()` so a
          rewrite operation can emit DELETED-status entries
          for the old files
        - Add a way to feed compacted outputs into the producer
          outside the FastAppend path (e.g. a public
          `add_data_files()` on a new RewriteFilesAction)
        - `SnapshotProducer::summary()` only walks
          `added_data_files` when populating the
          `SnapshotSummaryCollector`, so totals like
          `deleted-data-files` / `deleted-records` /
          `removed-files-size` (constant names from
          iceberg/src/spec/snapshot_summary.rs) are never
          produced. A parallel "removed/replaced source files"
          track has to be fed into the summary collector before
          a compaction snapshot's metrics are correct.
        - `Operation::Replace` is defined in
          iceberg/src/spec/snapshot.rs, but
          `update_snapshot_summaries()`
          [iceberg/src/spec/snapshot_summary.rs] currently
          rejects anything other than Append/Overwrite/Delete,
          so replace-summary support must be added before a
          compaction snapshot can be summarised
```

### 2.3 ManifestEntry Status

```
ManifestStatus enum                                 [iceberg/src/spec/manifest/entry.rs]
  │
  ├── Added       file is being added in this snapshot
  ├── Existing    file already existed in the previous snapshot
  └── Deleted     file is being removed in this snapshot

  Compaction would:
  1. Newly written compacted data files → ManifestEntry status = Added
  2. Old (rewritten) source data files  → ManifestEntry status = Deleted
                                          (emitted in a NEW manifest;
                                           see §2.2 — `delete_entries()`
                                           is the trait hook for this)
  3. Untouched files in carried-forward ManifestFiles keep whatever
     status they already had (typically Existing once the snapshot
     that Added them has rolled over). The producer does NOT rewrite
     statuses inside those manifests.
```

### 2.4 File Metrics Evaluators

```
Evaluators that could identify compaction candidates:

InclusiveMetricsEvaluator                           [iceberg/src/expr/visitors/]
  └─> Can evaluate predicates against DataFile statistics
      → Useful for filtering files to compact by partition or value range

ManifestEvaluator                                   [iceberg/src/expr/visitors/]
  └─> Can evaluate predicates against manifest-level partition summaries
      → Useful for partition-level file selection

DataFile                                            [iceberg/src/spec/manifest/data_file.rs]
  ├── file_size_in_bytes        → identify small files
  └── record_count              → identify tiny files

  NOTE: `DataFile` itself carries no associated-delete count or
  collection. Applicable deletes are matched per data file by
  `DeleteFileIndex` [iceberg/src/delete_file_index.rs] during
  scan planning and surfaced on `FileScanTask::deletes`. A
  delete-aware compaction selector (e.g. "files with > N
  deletes" or "files with > X% deleted rows") therefore needs
  to drive selection from `DeleteFileIndex` output rather than
  from `DataFile` alone — that selector does not exist today.
```

### 2.5 Read + Write Paths (and their gaps for rewrite)

```
The existing read and write infrastructure covers the data plane:

READ:  TableScan.plan_files() → ArrowReader.read() → RecordBatchStream
WRITE: IcebergWriter.write(batch) → close() → Vec<DataFile>

Gap for rewrite/commit:

`TableScan::plan_files()` returns `FileScanTaskStream` of
`FileScanTask` [iceberg/src/scan/task.rs]. `FileScanTask` only
carries `data_file_path`, `file_size_in_bytes`, `record_count`,
projection/predicate, partition info, and `deletes:
Vec<FileScanTaskDeleteFile>`. It does NOT carry the originating
`DataFile` or `ManifestEntry`, so a commit step cannot use the
scan stream alone to emit DELETED-status manifest entries for
the rewritten files.

Missing for rewrite to use:
- A planning path that yields the source `DataFile` /
  `ManifestEntry` for each candidate (an internal manifest-entry
  walk, or a new "rewrite plan" API distinct from the read-path
  `FileScanTask`)
- A wiring of those entries through a `SnapshotProduceOperation`
  that emits DELETED entries (see §2.2 gaps)

A compaction implementation would:
1. Walk manifests to identify candidate `DataFile` /
   `ManifestEntry` pairs (selection step)
2. Reuse `TableScan` / `ArrowReader` on those paths to read
   data with pending deletes applied
3. Write new compacted files via `IcebergWriter`
4. Commit: feed the original entries back to a
   `SnapshotProduceOperation` so old files become DELETED and
   new files become ADDED in one snapshot
```

---

## 3. Conceptual Compaction Flow (How It Would Work)

```
┌──────────────────────────────────────────────────────────────────────┐
│  HYPOTHETICAL: rewrite_data_files(table, options)                    │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                    ┌───────────V───────────────────────┐
                    │  1. REWRITE PLANNING              │
                    │     (NEW — does not exist today,  │
                    │      `TableScan::plan_files()`    │
                    │      yields FileScanTasks without │
                    │      the source DataFile /        │
                    │      ManifestEntry — see §2.5)    │
                    │                                   │
                    │  Walk current snapshot's manifests│
                    │  → collect (ManifestEntry,        │
                    │             DataFile) pairs       │
                    │    │                              │
                    │    ├─> Filter candidates:         │
                    │    │   file_size < min_size       │
                    │    │   file_size > max_size       │
                    │    │   delete_count > thresh      │
                    │    │   (delete count via          │
                    │    │    DeleteFileIndex — see     │
                    │    │    §2.4)                     │
                    │    │                              │
                    │    ├─> Group by partition         │
                    │    │                              │
                    │    └─> Bin-pack into groups       │
                    │        (first-fit-decreasing)     │
                    └───────────┬───────────────────────┘
                                │
                    ┌───────────V───────────────────────┐
                    │  2. FILE REWRITING                │
                    │     (reuses existing read+write   │
                    │      paths from §2.5)             │
                    │                                   │
                    │  For each group:                  │
                    │    READ:                          │
                    │      Build FileScanTasks for the  │
                    │      candidate paths and feed     │
                    │      them to ArrowReader; deletes │
                    │      are applied during read so   │
                    │      the stream is clean data.    │
                    │                                   │
                    │    WRITE:                         │
                    │      DataFileWriter               │
                    │      → new Parquet files          │
                    │      (target file size)           │
                    └───────────┬───────────────────────┘
                                │
                    ┌───────────V───────────────────────┐
                    │  3. METADATA COMMIT               │
                    │                                   │
                    │  RewriteFilesAction (NEW):        │
                    │    take ManifestEntry rows for    │
                    │      the rewritten source files   │
                    │    take new DataFiles from the    │
                    │      writers                      │
                    │    .commit()                      │
                    │    │                              │
                    │    V                              │
                    │  SnapshotProducer + a new         │
                    │  SnapshotProduceOperation that:   │
                    │    - returns Operation::Replace   │
                    │    - emits DELETED entries via    │
                    │      delete_entries() (must be    │
                    │      wired in — see §2.2)         │
                    │    - emits ADDED entries from     │
                    │      added_data_files             │
                    │    - carries forward untouched    │
                    │      ManifestFiles via            │
                    │      existing_manifest()          │
                    │    │                              │
                    │    V                              │
                    │  Catalog.update_table()           │
                    │    atomic CAS on metadata         │
                    └───────────────────────────────────┘
```

---

## 4. Strategy Comparison (Java Reference)

The Java implementation supports three compaction strategies. None exist in Rust yet:

```
┌──────────────┬─────────────────┬──────────────────┬─────────────────┐
│              │    BIN-PACK     │     SORT         │    Z-ORDER      │
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Goal         │ Consolidate     │ Consolidate +    │ Consolidate +   │
│              │ small files     │ sort data        │ multi-dim sort  │
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Shuffle      │ None            │ Range partition  │ Range partition │
│              │ (read + write)  │ + sort           │ + z-order       │
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Speed        │ Fastest         │ Slower (shuffle) │ Slower (shuffle)│
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Read benefit │ Fewer files     │ Fewer files +    │ Fewer files +   │
│              │ to open         │ better min/max   │ multi-column    │
│              │                 │ pruning on sort  │ pruning         │
│              │                 │ columns          │                 │
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Use when     │ Many small      │ Queries filter   │ Queries filter  │
│              │ files, no       │ on known         │ on multiple     │
│              │ specific query  │ columns          │ columns equally │
│              │ pattern         │                  │                 │
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Rust status  │ NOT IMPLEMENTED │ NOT IMPLEMENTED  │ NOT IMPLEMENTED │
└──────────────┴─────────────────┴──────────────────┴─────────────────┘
```

---

## 5. Java vs Rust Component Mapping

| Java Component                     | Purpose                         | Rust Equivalent                   | Status          |
|------------------------------------|---------------------------------|-----------------------------------|-----------------|
| `RewriteDataFilesSparkAction`      | Orchestrates compaction         | (none)                            | Not implemented |
| `BinPackRewriteFilePlanner`        | Size-based file selection       | (none)                            | Not implemented |
| `SparkShufflingDataRewritePlanner` | Sort/ZOrder file selection      | (none)                            | Not implemented |
| `BinPacking.ListPacker`            | First-fit-decreasing packing    | (none)                            | Not implemented |
| `SparkBinPackFileRewriteRunner`    | Read-write execution (BinPack)  | (none)                            | Not implemented |
| `SparkSortFileRewriteRunner`       | Read-write execution (Sort)     | (none)                            | Not implemented |
| `SparkZOrderFileRewriteRunner`     | Read-write execution (ZOrder)   | (none)                            | Not implemented |
| `RewriteDataFilesCommitManager`    | Commit coordination             | (none)                            | Not implemented |
| `BaseRewriteFiles`                 | Atomic file swap operation      | (none)                            | Not implemented |
| `SnapshotProducer`                 | Snapshot creation               | `SnapshotProducer`                | Exists          |
| `TransactionAction` (trait)        | Pluggable transaction action    | `TransactionAction` (trait)       | Exists          |
| `ManifestWriter`                   | Manifest file writing           | `ManifestWriter`                  | Exists          |
| `ManifestListWriter`               | Manifest list writing           | `ManifestListWriter`              | Exists          |
| `TableScan.planFiles()`            | File scanning                   | `TableScan::plan_files()`         | Exists          |
| `ParquetReader`                    | Data file reading               | `ArrowReader`                     | Exists          |
| `ParquetWriter`                    | Data file writing               | `ParquetWriter`                   | Exists          |
| `DataFile`                         | File metadata                   | `DataFile`                        | Exists          |
| `ManifestEntry` (status tracking)  | Added/Existing/Deleted          | `ManifestEntry` (ManifestStatus)  | Exists          |
| `InclusiveMetricsEvaluator`        | File stats evaluation           | `InclusiveMetricsEvaluator`       | Exists          |
| `ExpireSnapshots`                  | Old snapshot cleanup            | (none)                            | Not implemented |
| `RemoveOrphanFiles`                | Orphan file cleanup             | (none)                            | Not implemented |

---

## 6. Configuration Options (Java Reference)

These options would need Rust equivalents when compaction is implemented:

| Option                               | Default          | Purpose                                        |
|--------------------------------------|------------------|------------------------------------------------|
| `target-file-size-bytes`             | from table props | Target output file size                        |
| `min-file-size-bytes`                | 75% of target    | Files smaller than this are candidates         |
| `max-file-size-bytes`                | 180% of target   | Files larger than this are candidates          |
| `min-input-files`                    | 5                | Min files in a group to justify rewrite        |
| `max-file-group-size-bytes`          | 100 GB           | Max total size per rewrite group               |
| `max-concurrent-file-group-rewrites` | 5                | Parallel rewrite groups                        |
| `partial-progress.enabled`           | false            | Commit each group independently                |
| `delete-file-threshold`              | MAX_INT          | Files with N+ delete files are rewritten       |
| `delete-ratio-threshold`             | 0.3              | Files with 30%+ deleted rows are rewritten     |

---

## 7. Implementation Roadmap (What Would Be Needed)

To implement compaction in iceberg-rust, the following components would be needed:

```
1. RewriteFilesAction (transaction/rewrite_files.rs)
   └─> TransactionAction that marks old files as DELETED
       and adds new files as ADDED in a single atomic commit

2. File Selection / Planning
   └─> Identify candidate files by:
       - Size thresholds (too small / too large)
       - Delete file count / ratio
       - Partition grouping

3. Bin-Packing Algorithm
   └─> Group candidate files into rewrite groups
       respecting max group size constraints
       (first-fit-decreasing is standard approach)

4. Rewrite Execution
   └─> For each group:
       - Read via ArrowReader (applying deletes)
       - Write via DataFileWriter (new Parquet files)

5. Commit Manager
   └─> Coordinate commits across groups
       Support partial progress (commit per group)
       Handle conflicts with concurrent operations

6. Table Maintenance
   └─> ExpireSnapshots: remove old snapshots after compaction
   └─> RemoveOrphanFiles: delete unreferenced physical files
```
