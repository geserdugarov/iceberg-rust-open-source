# Iceberg Rust Compaction (RewriteDataFiles) — Call Stack

This document describes the state of compaction support in the Rust Iceberg implementation and how it compares to the Java implementation.

**Related docs:** [architecture_overview.md](architecture_overview.md) | [write_path_callstack.md](write_path_callstack.md) | [read_path_callstack.md](read_path_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md)

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
  │     └── ReplaceSortOrderAction    (change sort order)
  │
  └─> A "RewriteFilesAction" could be added following this pattern
```

### 2.2 SnapshotProducer

```
SnapshotProducer                                    [iceberg/src/transaction/snapshot.rs]
  │
  ├─> Can produce new snapshots with:
  │     - Added manifest entries (status: ADDED)
  │     - Existing manifest entries (status: EXISTING)
  │     - The trait SnapshotProduceOperation allows custom logic
  │       for determining which manifests to carry forward
  │
  └─> For compaction, a new operation type would need to:
        - Mark old files as DELETED in manifests
        - Add new compacted files as ADDED
        - Create snapshot with operation = "replace"
```

### 2.3 ManifestEntry Status

```
ManifestStatus enum                                 [iceberg/src/spec/manifest/mod.rs]
  │
  ├── Added       files added in this snapshot
  ├── Existing    files from previous snapshots
  └── Deleted     files removed in this snapshot

  Compaction would:
  1. Read data files (old, small files) → ManifestEntry status = Existing
  2. Write new data files (compacted) → ManifestEntry status = Added
  3. Mark old files → ManifestEntry status = Deleted
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
  ├── record_count              → identify tiny files
  └── (associated delete files) → identify files with many deletes
```

### 2.5 Complete Read + Write Paths

```
The existing read and write infrastructure can be composed for compaction:

READ:  TableScan.plan_files() → ArrowReader.read() → RecordBatchStream
WRITE: IcebergWriter.write(batch) → close() → Vec<DataFile>

A compaction implementation would:
1. Scan table to identify candidate files
2. Read data from those files (applying any pending deletes)
3. Write new compacted files
4. Commit: remove old files + add new files atomically
```

---

## 3. Conceptual Compaction Flow (How It Would Work)

```
┌──────────────────────────────────────────────────────────────────────┐
│  HYPOTHETICAL: rewrite_data_files(table, options)                    │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                    ┌───────────V───────────────────┐
                    │  1. FILE PLANNING             │
                    │                               │
                    │  table.scan()                 │
                    │    .plan_files()              │
                    │    │                          │
                    │    ├─> Filter candidates:     │
                    │    │   file_size < min_size   │
                    │    │   file_size > max_size   │
                    │    │   delete_count > thresh  │
                    │    │                          │
                    │    ├─> Group by partition     │
                    │    │                          │
                    │    └─> Bin-pack into groups   │
                    │        (first-fit-decreasing) │
                    └───────────┬───────────────────┘
                                │
                    ┌───────────V───────────────────┐
                    │  2. FILE REWRITING            │
                    │                               │
                    │  For each group:              │
                    │    READ:                      │
                    │      ArrowReader.read(tasks)  │
                    │      (deletes applied during  │
                    │       read → clean data)      │
                    │                               │
                    │    WRITE:                     │
                    │      DataFileWriter           │
                    │      → new Parquet files      │
                    │      (target file size)       │
                    └───────────┬───────────────────┘
                                │
                    ┌───────────V───────────────────┐
                    │  3. METADATA COMMIT           │
                    │                               │
                    │  RewriteFilesAction:          │
                    │    delete_file(old_files...)  │
                    │    add_file(new_files...)     │
                    │    .commit()                  │
                    │    │                          │
                    │    V                          │
                    │  SnapshotProducer:            │
                    │    manifest with DELETED      │
                    │    manifest with ADDED        │
                    │    manifest list              │
                    │    snapshot (op = "replace")  │
                    │    │                          │
                    │    V                          │
                    │  Catalog.update_table()       │
                    │    atomic CAS on metadata     │
                    └───────────────────────────────┘
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
