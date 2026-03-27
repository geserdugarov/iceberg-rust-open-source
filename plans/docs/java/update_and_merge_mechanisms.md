# Iceberg UPDATE and MERGE INTO (Upsert) Mechanisms

This document provides a comprehensive description of how Apache Iceberg implements row-level UPDATE and MERGE INTO (upsert) operations in Spark, covering both Copy-on-Write and Merge-on-Read strategies.

**Related docs:** [architecture_overview.md](architecture_overview.md) | [read_path_callstack.md](read_path_callstack.md) | [write_path_callstack.md](write_path_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md) | [compaction_callstack.md](compaction_callstack.md)

---

## 1. Overview

### UPDATE

Modifies existing rows in a single Iceberg table based on a WHERE condition:

```sql
UPDATE catalog.db.table
SET value = 'new_value', updated_at = current_timestamp()
WHERE id = 42
```

### MERGE INTO (Upsert)

Joins a source dataset against a target Iceberg table and applies conditional INSERT, UPDATE, and DELETE in a single atomic transaction:

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.value = s.value, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (id, value, updated_at) VALUES (s.id, s.value, s.updated_at)
WHEN NOT MATCHED BY SOURCE AND t.updated_at < '2024-01-01' THEN DELETE
```

### Execution Strategies

Both operations use the same two strategies as DELETE (see [delete_mechanisms.md](delete_mechanisms.md)):

- **Copy-on-Write (CoW):** Read affected data files, apply modifications, write new files with updated rows. Old files replaced in one atomic commit.
- **Merge-on-Read (MoR):** Write position delete files (or DVs) for old rows + new data files for updated/inserted rows. Deletes applied at read time.

### UPDATE vs MERGE Comparison

```
┌────────────────────┬──────────────────────────┬──────────────────────────────┐
│                    │  UPDATE                  │  MERGE INTO                  │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ SQL                │ Single table + WHERE     │ Source-target JOIN +         │
│                    │                          │ WHEN MATCHED / NOT MATCHED   │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ Logical Plan       │ UpdateTable              │ MergeIntoTable               │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ Can INSERT?        │ No                       │ Yes (WHEN NOT MATCHED)       │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ Can DELETE?        │ No                       │ Yes (WHEN MATCHED THEN       │
│                    │                          │ DELETE)                      │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ CoW metadata cols  │ _file + _pos             │ _file only                   │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ CoW distribution   │ File-aware clustering    │ Standard partition           │
│                    │ (by _file)               │ clustering                   │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ MoR behavior       │ DELETE old + INSERT new  │ DELETE old + INSERT new      │
│                    │ (identical)              │ + INSERT-only for unmatched  │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ Config property    │ write.update.mode        │ write.merge.mode             │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ Isolation property │ write.update.isolation-  │ write.merge.isolation-       │
│                    │ level                    │ level                        │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ Commit API (CoW)   │ OverwriteFiles           │ OverwriteFiles               │
├────────────────────┼──────────────────────────┼──────────────────────────────┤
│ Commit API (MoR)   │ RowDelta                 │ RowDelta                     │
└────────────────────┴──────────────────────────┴──────────────────────────────┘
```

---

## 2. High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│  SQL:  UPDATE table SET col=val WHERE cond                              │
│        MERGE INTO target USING source ON cond WHEN MATCHED ...          │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                    ┌──────────▼────────────┐
                    │  SPARK SQL PARSER     │
                    │                       │
                    │  UPDATE → UpdateTable │
                    │  MERGE  → MergeInto   │
                    │          Table        │
                    └──────────┬────────────┘
                               │
                    ┌──────────▼────────────┐
                    │  SPARK ANALYZER       │
                    │                       │
                    │  Row lineage rules:   │
                    │  RewriteUpdateTable   │
                    │  ForRowLineage        │
                    │  RewriteMergeInto     │
                    │  TableForRowLineage   │
                    └──────────┬────────────┘
                               │
                    ┌──────────▼────────────┐
                    │  SparkTable           │
                    │  .newRowLevelOp-      │
                    │   erationBuilder()    │
                    └──────────┬────────────┘
                               │
                    ┌──────────▼────────────┐
                    │  SparkRowLevel-       │
                    │  OperationBuilder     │
                    │                       │
                    │  Read mode from       │
                    │  table properties:    │
                    │  write.update.mode    │
                    │  write.merge.mode     │
                    └──────┬───────┬────────┘
                           │       │
            ┌──────────────┘       └───────────────────┐
            │ COPY_ON_WRITE                            │ MERGE_ON_READ
            │                                          │
            ▼                                          ▼
┌───────────────────────┐               ┌──────────────────────────┐
│ SparkCopyOnWrite-     │               │ SparkPositionDelta-      │
│ Operation             │               │ Operation                │
│                       │               │                          │
│ SCAN:                 │               │ SCAN:                    │
│  SparkCopyOnWriteScan │               │  SparkBatchQueryScan     │
│  (reads affected      │               │  (reads rows with        │
│   files entirely)     │               │   _file + _pos metadata) │
│                       │               │                          │
│ WRITE:                │               │ WRITE:                   │
│  Rewrite files with   │               │  Position deletes for    │
│  modified rows        │               │  old rows + new data     │
│                       │               │  files for updated rows  │
│                       │               │                          │
│ COMMIT:               │               │ COMMIT:                  │
│  OverwriteFiles       │               │  RowDelta                │
│  (delete old files    │               │  .addDeletes(deleteFile) │
│   + add new files)    │               │  .addRows(dataFile)      │
└───────────────────────┘               └──────────────────────────┘
```

---

## 3. Copy-on-Write UPDATE

### 3.1 Complete Call Stack

```
UPDATE catalog.db.table SET value = 'X' WHERE id = 42
       │
       ▼
Spark SQL Parser → UpdateTable logical plan               [Spark Core]
       │
       ▼
RewriteUpdateTableForRowLineage                           [spark-extensions]
  └─► Inject row lineage assignments:
        _row_id = _row_id (preserve)
        _last_updated_sequence_number = null (mark as updated)
       │
       ▼
SparkTable.newRowLevelOperationBuilder(info)              [spark/source/SparkTable]
  └─► new SparkRowLevelOperationBuilder(table, info)
        │
        ├─► mode = tableProperties.get("write.update.mode")
        │     → default: "copy-on-write"
        │
        └─► return new SparkCopyOnWriteOperation(...)     [COPY_ON_WRITE path]

SparkCopyOnWriteOperation                                [spark/source]
  │
  ├─► SCAN PHASE:
  │     newScanBuilder()
  │       └─► SparkScanBuilder.buildCopyOnWriteScan()
  │             └─► SparkCopyOnWriteScan
  │                   │
  │                   ├─► Required metadata:
  │                   │     _file (FILE_PATH)             file identification
  │                   │     _pos (ROW_POSITION)           row-level targeting
  │                   │     _row_id (optional)            row lineage
  │                   │
  │                   ├─► Captures scan snapshot ID
  │                   │     (baseline for conflict detection)
  │                   │
  │                   └─► Supports runtime filtering:
  │                         Spark pushes In(_file_path, [list])
  │                         to narrow scan to only affected files
  │
  ├─► SPARK EXECUTION:
  │     1. Scan affected files with metadata columns
  │     2. Apply WHERE filter → identify rows to update
  │     3. Apply SET expressions → compute new column values
  │     4. Output ALL rows from affected files:
  │        - Updated rows (with new values)
  │        - Unchanged rows (passed through as-is)
  │        (entire file must be rewritten, not just changed rows)
  │
  ├─► WRITE PHASE:
  │     newWriteBuilder()
  │       └─► SparkWriteBuilder.overwriteFiles(scan, command)
  │             └─► SparkWrite.CopyOnWriteOperation
  │
  │     Distribution (SparkWriteUtil):
  │       UPDATE uses file-aware distribution:
  │         HASH mode: cluster by (_file) or (_file, _partition)
  │         RANGE mode: order by (_file, _pos)
  │         → groups rows from same file together for efficient rewrite
  │
  │     Writers:
  │       Same writer infrastructure as INSERT
  │       (UnpartitionedDataWriter / PartitionedDataWriter)
  │       → produce new Parquet data files
  │
  └─► COMMIT PHASE:
        SparkWrite.CopyOnWriteOperation.commit()         [spark/source/SparkWrite]
          │
          ├─► Collect overwrittenFiles from scan tasks:
          │     (original data files that were read and rewritten)
          │
          ├─► OverwriteFiles overwrite = table.newOverwrite()
          │
          ├─► overwrite.deleteFiles(overwrittenFiles)     remove old files
          │
          ├─► for each new DataFile:
          │     overwrite.addFile(newFile)                 add rewritten files
          │
          ├─► Validation (depends on isolation level):
          │     │
          │     ├─► SERIALIZABLE:
          │     │     overwrite.validateFromSnapshot(scanSnapshotId)
          │     │     overwrite.conflictDetectionFilter(combinedFilter)
          │     │     overwrite.validateNoConflictingData()
          │     │     overwrite.validateNoConflictingDeletes()
          │     │
          │     └─► SNAPSHOT:
          │           overwrite.validateFromSnapshot(scanSnapshotId)
          │           overwrite.conflictDetectionFilter(combinedFilter)
          │           overwrite.validateNoConflictingDeletes()
          │
          └─► overwrite.commit()
                └─► SnapshotProducer.commit()
                      atomic CAS on metadata.json
```

### 3.2 CoW UPDATE Data Flow Example

```
BEFORE:                                 DURING (Spark execution):
┌────────────────────┐                  ┌────────────────────────────────────┐
│ data-file-001      │                  │ Scan data-file-001:                │
│                    │                  │   Row 0: {id=41, val="A"} → pass   │
│ Row 0: id=41 "A"   │                  │   Row 1: {id=42, val="B"} → MATCH  │
│ Row 1: id=42 "B"  ←── UPDATE          │          SET val="X"               │
│ Row 2: id=43 "C"   │                  │   Row 2: {id=43, val="C"} → pass   │
│                    │                  │                                    │
└────────────────────┘                  │ Output ALL rows (modified + not):  │
                                        │   {id=41, val="A"} (unchanged)     │
                                        │   {id=42, val="X"} (updated)       │
                                        │   {id=43, val="C"} (unchanged)     │
                                        └────────────────────────────────────┘

AFTER:
┌────────────────────┐
│ data-file-001      │  ← DELETED (old file removed)
└────────────────────┘
┌────────────────────┐
│ data-file-002      │  ← NEW (rewritten with all rows)
│                    │
│ Row 0: id=41 "A"   │
│ Row 1: id=42 "X"   │  ← updated value
│ Row 2: id=43 "C"   │
└────────────────────┘

Commit: OverwriteFiles
  .deleteFile(data-file-001)
  .addFile(data-file-002)
  .commit()
```

---

## 4. Copy-on-Write MERGE INTO

### 4.1 Complete Call Stack

```
MERGE INTO target t USING source s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET t.value = s.value
  WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
       │
       ▼
Spark SQL Parser → MergeIntoTable logical plan            [Spark Core]
  │  matchedActions: [UpdateAction(SET t.value = s.value)]
  │  notMatchedActions: [InsertAction(s.id, s.value)]
  │  notMatchedBySourceActions: []
       │
       ▼
RewriteMergeIntoTableForRowLineage                        [spark-extensions]
  └─► For each UpdateAction:
        Inject: _row_id = _row_id, _last_updated_sequence_number = null
       │
       ▼
SparkRowLevelOperationBuilder                             [spark/source]
  └─► mode = tableProperties.get("write.merge.mode")
        → default: "copy-on-write"
        → return SparkCopyOnWriteOperation(...)
       │
       ▼
SparkCopyOnWriteOperation                                [spark/source]
  │
  ├─► SCAN PHASE:
  │     SparkCopyOnWriteScan
  │       Required metadata: _file (for MERGE, no _pos needed)
  │       Runtime filtering: In(_file_path, [affected files])
  │
  ├─► SPARK EXECUTION (JOIN + CLAUSE EVALUATION):
  │     │
  │     ├─► 1. Join target ⋈ source ON t.id = s.id
  │     │
  │     ├─► 2. For each result row:
  │     │     │
  │     │     ├─► Target-Source match (WHEN MATCHED):
  │     │     │     Evaluate conditions in clause order (first match wins):
  │     │     │       WHEN MATCHED AND cond1 THEN UPDATE SET ...
  │     │     │       WHEN MATCHED AND cond2 THEN DELETE
  │     │     │       WHEN MATCHED THEN UPDATE SET ...  (catch-all)
  │     │     │
  │     │     ├─► Source-only row (WHEN NOT MATCHED):
  │     │     │     New row to INSERT into target
  │     │     │
  │     │     └─► Target-only row (WHEN NOT MATCHED BY SOURCE):
  │     │           Apply UPDATE or DELETE to target row
  │     │
  │     └─► 3. Output: ALL rows from affected files
  │           (matched-updated + matched-deleted-excluded
  │            + unmatched-target-passthrough + new inserts)
  │
  ├─► WRITE PHASE:
  │     Distribution (SparkWriteUtil):
  │       MERGE uses standard partition distribution:
  │         HASH mode: cluster by (_partition)
  │         → NOT file-aware (join shuffles rows across files)
  │
  │     Writers: produce new Parquet data files
  │
  └─► COMMIT PHASE:
        Same as CoW UPDATE:
          OverwriteFiles
            .deleteFile(affectedFiles...)
            .addFile(newFiles...)
            + validation based on isolation level
            .commit()
```

### 4.2 CoW MERGE Data Flow Example

```
Target:                    Source:
┌──────────────────┐       ┌──────────────────┐
│ data-file-001    │       │ source DataFrame │
│ id=1, val="A"    │       │ id=1, val="A2"   │  ← match → UPDATE
│ id=2, val="B"    │       │ id=3, val="C"    │  ← no match → INSERT
│                  │       │                  │
└──────────────────┘       └──────────────────┘

After MERGE (CoW):
┌──────────────────┐       (data-file-001 deleted)
│ data-file-002    │
│ id=1, val="A2"   │       ← updated via WHEN MATCHED
│ id=2, val="B"    │       ← unchanged (passthrough)
│ id=3, val="C"    │       ← inserted via WHEN NOT MATCHED
└──────────────────┘

Commit: OverwriteFiles
  .deleteFile(data-file-001)
  .addFile(data-file-002)
```

---

## 5. Merge-on-Read UPDATE

### 5.1 Complete Call Stack

```
UPDATE catalog.db.table SET value = 'X' WHERE id = 42
       │                                           (write.update.mode = merge-on-read)
       ▼
SparkRowLevelOperationBuilder                             [spark/source]
  └─► mode = "merge-on-read"
        → return SparkPositionDeltaOperation(...)

SparkPositionDeltaOperation                              [spark/source]
  │
  │  Implements SupportsDelta:
  │    rowId() = [_file, _pos]                           row identification
  │    representUpdateAsDeleteAndInsert() = true         decompose UPDATE
  │
  ├─► SCAN PHASE:
  │     newScanBuilder()
  │       └─► SparkScanBuilder.buildMergeOnReadScan()
  │             └─► SparkBatchQueryScan
  │                   Required metadata:
  │                     _spec_id                          partition spec routing
  │                     _partition                        partition values
  │                     _row_id (optional)                row lineage
  │
  ├─► SPARK EXECUTION (DELTA DECOMPOSITION):
  │     │
  │     ├─► 1. Scan target table with _file + _pos metadata
  │     │
  │     ├─► 2. Apply WHERE filter → find rows to update
  │     │
  │     ├─► 3. Decompose each UPDATE into DELETE + INSERT:
  │     │     │
  │     │     │  For row at (data-file-001, pos=17): id=42, val="B"
  │     │     │
  │     │     ├─► DELETE marker:
  │     │     │     (_file="data-file-001", _pos=17)
  │     │     │
  │     │     └─► INSERT row:
  │     │           {id=42, val="X", ...}  (with new values from SET)
  │     │
  │     └─► 4. Output delta stream:
  │           delete markers + new data rows
  │
  ├─► WRITE PHASE:
  │     SparkPositionDeltaWrite                          [spark/source]
  │       │
  │       ├─► PositionDeltaWriteFactory.createWriter()
  │       │     │
  │       │     ├─► if unpartitioned:
  │       │     │     new UnpartitionedDeltaWriter(dataWriter, deleteWriter)
  │       │     │
  │       │     └─► if partitioned:
  │       │           new PartitionedDeltaWriter(dataWriter, deleteWriter)
  │       │
  │       │  Each delta writer has TWO sub-writers:
  │       │    dataWriter:   writes new data files (INSERT rows)
  │       │    deleteWriter: writes delete files (DELETE markers)
  │       │
  │       ├─► Delete writer selection:
  │       │     │
  │       │     ├─► if context.useDVs():                 V3+ table
  │       │     │     PartitioningDVWriter
  │       │     │       → Puffin file with RoaringBitmap blobs
  │       │     │
  │       │     ├─► elif inputOrdered && no rewritable deletes:
  │       │     │     ClusteredPositionDeleteWriter       V2 table
  │       │     │       → Parquet file with (file_path, pos)
  │       │     │
  │       │     └─► else:
  │       │           FanoutPositionOnlyDeleteWriter      V2 unordered
  │       │           → per-file Parquet delete files
  │       │
  │       └─► Distribution (SparkWriteUtil):
  │             HASH mode: cluster by (_spec_id, _partition, _file)
  │             Ordering: (_spec_id, _partition, _file, _pos)
  │             → groups delete markers by file for efficient delete files
  │
  └─► COMMIT PHASE:
        SparkPositionDeltaWrite.commit(messages)         [spark/source]
          │
          ├─► RowDelta rowDelta = table.newRowDelta()
          │
          ├─► for each DeltaTaskCommit message:
          │     │
          │     ├─► for each DataFile in message.dataFiles():
          │     │     rowDelta.addRows(dataFile)          new data with updated values
          │     │
          │     ├─► for each DeleteFile in message.deleteFiles():
          │     │     rowDelta.addDeletes(deleteFile)     position deletes / DVs
          │     │
          │     └─► for each DeleteFile in message.rewrittenDeleteFiles():
          │           rowDelta.removeDeletes(deleteFile)  cleanup merged DVs
          │
          ├─► rowDelta.validateDataFilesExist(referencedDataFiles)
          │
          ├─► Validation (UPDATE-specific):
          │     rowDelta.validateDeletedFiles()
          │     rowDelta.validateNoConflictingDeleteFiles()
          │     if SERIALIZABLE:
          │       rowDelta.validateNoConflictingDataFiles()
          │
          └─► rowDelta.commit()
                └─► SnapshotProducer.commit()
```

### 5.2 MoR UPDATE Data Flow Example

```
BEFORE:                                 AFTER:
┌────────────────────┐                  ┌────────────────────┐
│ data-file-001      │                  │ data-file-001      │  (UNCHANGED)
│                    │                  │                    │
│ Row 0: id=41 "A"   │                  │ Row 0: id=41 "A"   │
│ Row 1: id=42 "B"  ◄── UPDATE          │ Row 1: id=42 "B"   │  (logically deleted)
│ Row 2: id=43 "C"   │                  │ Row 2: id=43 "C"   │
└────────────────────┘                  └────────────────────┘
                                        ┌────────────────────┐
                                        │ delete-file-001    │  (NEW: position delete)
                                        │ (data-file-001,    │
                                        │  pos=1)            │
                                        └────────────────────┘
                                        ┌────────────────────┐
                                        │ data-file-002      │  (NEW: updated row)
                                        │ id=42, val="X"     │
                                        └────────────────────┘

Commit: RowDelta
  .addDeletes(delete-file-001)    position delete for old row
  .addRows(data-file-002)         new file with updated row
  .commit()

At READ time:
  data-file-001 is read but row at pos=1 is filtered out (deleted)
  data-file-002 is read normally (new row with updated value)
  Result: id=41 "A", id=42 "X", id=43 "C"
```

---

## 6. Merge-on-Read MERGE INTO

### 6.1 Complete Call Stack

```
MERGE INTO target t USING source s ON t.id = s.id
  WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
  WHEN MATCHED THEN UPDATE SET t.value = s.value
  WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
       │                                           (write.merge.mode = merge-on-read)
       ▼
SparkPositionDeltaOperation                              [spark/source]
  │
  ├─► SCAN PHASE: same as MoR UPDATE
  │
  ├─► SPARK EXECUTION (JOIN + DELTA DECOMPOSITION):
  │     │
  │     ├─► 1. Join target ⋈ source ON t.id = s.id
  │     │
  │     ├─► 2. Evaluate clauses (first match wins per row):
  │     │
  │     │   WHEN MATCHED AND s.op = 'DELETE':
  │     │     → DELETE marker: (_file, _pos)                    delete only
  │     │
  │     │   WHEN MATCHED THEN UPDATE:
  │     │     → DELETE marker: (_file, _pos) for old row        delete + insert
  │     │     → INSERT row: {t.id, s.value, ...} for new row
  │     │
  │     │   WHEN NOT MATCHED THEN INSERT:
  │     │     → INSERT row: {s.id, s.value, ...}                insert only
  │     │
  │     └─► 3. Output: mixed delta stream
  │           (delete markers + data rows)
  │
  ├─► WRITE PHASE:
  │     SparkPositionDeltaWrite
  │       │
  │       ├─► Writer selection:
  │       │     │
  │       │     ├─► if command == DELETE (pure delete MERGE):
  │       │     │     DeleteOnlyDeltaWriter
  │       │     │       only deleteWriter (no dataWriter)
  │       │     │
  │       │     ├─► if unpartitioned:
  │       │     │     UnpartitionedDeltaWriter
  │       │     │       dataWriter + deleteWriter
  │       │     │
  │       │     └─► if partitioned:
  │       │           PartitionedDeltaWriter
  │       │           dataWriter + deleteWriter
  │       │           routes to partition-specific writers
  │       │
  │       └─► DeltaTaskCommit output:
  │             dataFiles[]              new inserts + update inserts
  │             deleteFiles[]            position deletes / DVs for matched rows
  │             rewrittenDeleteFiles[]   old DVs being replaced (if merging)
  │             referencedDataFiles[]    files referenced by delete markers
  │
  └─► COMMIT PHASE:
        Same as MoR UPDATE:
          RowDelta with validation
```

### 6.2 MoR MERGE Data Flow Example

```
Target:                       Source:
┌──────────────────────┐      ┌─────────────────────────┐
│ data-file-001        │      │ source DataFrame        │
│ Row 0: id=1 val="A"  │      │ id=1 op="UPD" val="A2"  │ ← MATCHED → UPDATE
│ Row 1: id=2 val="B"  │      │ id=2 op="DEL"           │ ← MATCHED → DELETE
│                      │      │ id=3          val="C"   │ ← NOT MATCHED → INSERT
└──────────────────────┘      └─────────────────────────┘

After MERGE (MoR):
┌──────────────────────┐
│ data-file-001        │  (UNCHANGED)
│ Row 0: id=1 val="A"  │  (logically deleted by position delete)
│ Row 1: id=2 val="B"  │  (logically deleted by position delete)
└──────────────────────┘
┌──────────────────────┐
│ delete-file-001      │  (NEW: position deletes)
│ (data-file-001, 0)   │  ← delete for UPDATE on id=1
│ (data-file-001, 1)   │  ← delete for DELETE on id=2
└──────────────────────┘
┌──────────────────────┐
│ data-file-002        │  (NEW: inserts + update-inserts)
│ id=1 val="A2"        │  ← re-inserted with updated value
│ id=3 val="C"         │  ← new insert
└──────────────────────┘

Commit: RowDelta
  .addDeletes(delete-file-001)
  .addRows(data-file-002)
  .commit()
```

---

## 7. Row Identification and Metadata Columns

### 7.1 Metadata Columns

| Column                          | Internal Name                                  | Type    | Purpose                               |
|---------------------------------|------------------------------------------------|---------|---------------------------------------|
| `_file`                         | `MetadataColumns.FILE_PATH`                    | string  | Data file path for row identification |
| `_pos`                          | `MetadataColumns.ROW_POSITION`                 | long    | 0-based row position within the file  |
| `_spec_id`                      | `MetadataColumns.SPEC_ID`                      | int     | Partition spec version for routing    |
| `_partition`                    | `MetadataColumns.PARTITION_COLUMN_NAME`        | struct  | Partition values for routing          |
| `_row_id`                       | `MetadataColumns.ROW_ID`                       | long    | Row lineage identity (optional)       |
| `_last_updated_sequence_number` | `MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER` | long    | Last modification tracking (optional) |
| `_is_deleted`                   | `MetadataColumns.IS_DELETED`                   | boolean | Delete marking for changelogs         |

### 7.2 Which Columns Are Used Where

```
┌──────────────────┬───────────────────────┬───────────────────────┐
│  Metadata Column │  CoW                  │  MoR                  │
├──────────────────┼───────────────────────┼───────────────────────┤
│  _file           │  UPDATE: yes (dist.)  │  yes (rowId)          │
│                  │  MERGE: yes (scan)    │                       │
├──────────────────┼───────────────────────┼───────────────────────┤
│  _pos            │  UPDATE: yes (dist.)  │  yes (rowId)          │
│                  │  MERGE: no            │                       │
├──────────────────┼───────────────────────┼───────────────────────┤
│  _spec_id        │  no                   │  yes (partition       │
│                  │                       │   routing)            │
├──────────────────┼───────────────────────┼───────────────────────┤
│  _partition      │  no                   │  yes (partition       │
│                  │                       │   routing)            │
├──────────────────┼───────────────────────┼───────────────────────┤
│  _row_id         │  optional (lineage)   │  optional (lineage)   │
├──────────────────┼───────────────────────┼───────────────────────┤
│  _last_updated_  │  optional (lineage)   │  optional (lineage)   │
│  sequence_number │                       │                       │
└──────────────────┴───────────────────────┴───────────────────────┘
```

### 7.3 Row Lineage

When a table supports row lineage (V3+ with row lineage enabled), Spark's analyzer rules inject additional assignments into UPDATE and MERGE actions:

```
RewriteUpdateTableForRowLineage / RewriteMergeIntoTableForRowLineage:
  For each UPDATE action:
    - Add assignment: _row_id = _row_id            (preserve original ID)
    - Add assignment: _last_updated_sequence_number = null  (mark as updated)
```

This enables downstream consumers to track row identity across updates.

---

## 8. MERGE INTO Multi-Clause Evaluation

### 8.1 Clause Types

```sql
MERGE INTO target t
USING source s
ON t.id = s.id

-- WHEN MATCHED: source row matched a target row (join hit)
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.op = 'UPDATE' THEN UPDATE SET t.value = s.value
WHEN MATCHED THEN UPDATE SET t.value = s.value         -- catch-all

-- WHEN NOT MATCHED: source row has no matching target (new row)
WHEN NOT MATCHED AND s.value IS NOT NULL THEN INSERT (id, value) VALUES (s.id, s.value)

-- WHEN NOT MATCHED BY SOURCE: target row has no matching source (orphan)
WHEN NOT MATCHED BY SOURCE AND t.updated_at < '2024-01-01' THEN DELETE
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.status = 'orphan'
```

### 8.2 Evaluation Order

```
For each row in join result:
  │
  ├─► Target-Source MATCH (both sides present):
  │     Evaluate matchedActions[] in ORDER:
  │       1. Check condition of first clause
  │          → if true: apply action (UPDATE or DELETE), STOP
  │       2. Check condition of second clause
  │          → if true: apply action, STOP
  │       3. ... (first match wins)
  │       N. If no clause matches: row passes through unchanged
  │
  ├─► Source-only row (NOT MATCHED):
  │     Evaluate notMatchedActions[] in ORDER:
  │       → first matching INSERT clause applied
  │       → if none match: row is discarded
  │
  └─► Target-only row (NOT MATCHED BY SOURCE):
        Evaluate notMatchedBySourceActions[] in ORDER:
        → first matching UPDATE or DELETE clause applied
        → if none match: row passes through unchanged
```

### 8.3 Multi-Clause Example

```sql
MERGE INTO inventory t USING updates s ON t.sku = s.sku
WHEN MATCHED AND s.qty = 0 THEN DELETE                    -- clause 1: remove zero-qty
WHEN MATCHED THEN UPDATE SET t.qty = s.qty                -- clause 2: update qty
WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (s.sku, s.qty)  -- clause 3: new SKU

-- For each matched row:
--   If s.qty = 0: clause 1 fires (DELETE) → clause 2 is NOT evaluated
--   If s.qty > 0: clause 1 skipped → clause 2 fires (UPDATE)
```

---

## 9. Isolation Levels and Conflict Detection

### 9.1 Configuration

| Property                       | Default        | Values                     |
|--------------------------------|----------------|----------------------------|
| `write.update.isolation-level` | `serializable` | `serializable`, `snapshot` |
| `write.merge.isolation-level`  | `serializable` | `serializable`, `snapshot` |

### 9.2 Validation by Strategy

**Copy-on-Write (OverwriteFiles):**

| Isolation      | Validation Steps                                                                    |
|----------------|-------------------------------------------------------------------------------------|
| `serializable` | `validateFromSnapshot(scanSnapshotId)`                                              |
|                | `conflictDetectionFilter(combinedFilter)`                                           |
|                | `validateNoConflictingData()` — fails if concurrent INSERT into affected partitions |
|                | `validateNoConflictingDeletes()` — fails if concurrent DELETE on affected files     |
| `snapshot`     | `validateFromSnapshot(scanSnapshotId)`                                              |
|                | `conflictDetectionFilter(combinedFilter)`                                           |
|                | `validateNoConflictingDeletes()` — fails if concurrent DELETE on affected files     |

**Merge-on-Read (RowDelta):**

| Isolation      | Validation Steps                                                                         |
|----------------|------------------------------------------------------------------------------------------|
| `serializable` | `validateFromSnapshot(scanSnapshotId)`                                                   |
|                | `validateDataFilesExist(referencedDataFiles)`                                            |
|                | `validateDeletedFiles()` — ensures data files not deleted concurrently                   |
|                | `validateNoConflictingDeleteFiles()` — fails if concurrent deletes on same rows          |
|                | `validateNoConflictingDataFiles()` — fails if concurrent INSERT into affected partitions |
| `snapshot`     | `validateFromSnapshot(scanSnapshotId)`                                                   |
|                | `validateDataFilesExist(referencedDataFiles)`                                            |
|                | `validateDeletedFiles()`                                                                 |
|                | `validateNoConflictingDeleteFiles()`                                                     |

### 9.3 Concurrent Operation Scenario Matrix

```
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

FAILS = commit is rejected, retried with exponential backoff
SUCCEEDS = commits proceed without conflict
```

**Key difference:**
- `SERIALIZABLE`: Prevents phantom reads — no concurrent INSERTs allowed into affected partitions
- `SNAPSHOT`: Allows concurrent INSERTs (acceptable phantom reads) — only prevents concurrent DELETEs/UPDATEs on the same data

---

## 10. Configuration

| Property                       | Default              | Values                           | Scope                                                                |
|--------------------------------|----------------------|----------------------------------|----------------------------------------------------------------------|
| `write.update.mode`            | `copy-on-write`      | `copy-on-write`, `merge-on-read` | UPDATE operations                                                    |
| `write.merge.mode`             | `copy-on-write`      | `copy-on-write`, `merge-on-read` | MERGE operations                                                     |
| `write.delete.mode`            | `copy-on-write`      | `copy-on-write`, `merge-on-read` | DELETE operations (see [delete_mechanisms.md](delete_mechanisms.md)) |
| `write.update.isolation-level` | `serializable`       | `serializable`, `snapshot`       | UPDATE conflict detection                                            |
| `write.merge.isolation-level`  | `serializable`       | `serializable`, `snapshot`       | MERGE conflict detection                                             |
| `write.distribution-mode`      | `hash`               | `none`, `hash`, `range`          | Shuffle strategy                                                     |
| `write.target-file-size-bytes` | `536870912` (512 MB) | long                             | Target data file size                                                |

Set via table properties:
```sql
ALTER TABLE catalog.db.table SET TBLPROPERTIES (
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read',
  'write.update.isolation-level' = 'snapshot'
);
```

---

## 11. Key Classes Reference

| Step                  | Class                                | Module           | Key Method                                                 |
|-----------------------|--------------------------------------|------------------|------------------------------------------------------------|
| **Entry point**       | `SparkTable`                         | spark/source     | `newRowLevelOperationBuilder()`                            |
| **Mode dispatch**     | `SparkRowLevelOperationBuilder`      | spark/source     | `build()` — reads mode from table properties               |
| **CoW operation**     | `SparkCopyOnWriteOperation`          | spark/source     | `newScanBuilder()`, `newWriteBuilder()`                    |
| **CoW scan**          | `SparkCopyOnWriteScan`               | spark/source     | `planFiles()`, runtime filtering support                   |
| **CoW commit**        | `SparkWrite.CopyOnWriteOperation`    | spark/source     | `commit()` — OverwriteFiles with validation                |
| **MoR operation**     | `SparkPositionDeltaOperation`        | spark/source     | `rowId()`, `representUpdateAsDeleteAndInsert()`            |
| **MoR write**         | `SparkPositionDeltaWrite`            | spark/source     | `toBatch()` → `PositionDeltaBatchWrite`                    |
| **MoR commit**        | `SparkPositionDeltaWrite`            | spark/source     | `commit()` — RowDelta with validation                      |
| **Delta commit msg**  | `DeltaTaskCommit`                    | spark/source     | `dataFiles[]`, `deleteFiles[]`, `rewrittenDeleteFiles[]`   |
| **MoR writers**       | `UnpartitionedDeltaWriter`           | spark/source     | Unpartitioned UPDATE/MERGE                                 |
|                       | `PartitionedDeltaWriter`             | spark/source     | Partitioned UPDATE/MERGE                                   |
|                       | `DeleteOnlyDeltaWriter`              | spark/source     | DELETE-only MERGE clauses                                  |
| **DV writer**         | `PartitioningDVWriter`               | core/io          | V3+ deletion vector writing                                |
| **Pos delete writer** | `ClusteredPositionDeleteWriter`      | core/io          | V2 ordered position deletes                                |
|                       | `FanoutPositionOnlyDeleteWriter`     | core/io          | V2 unordered position deletes                              |
| **Distribution**      | `SparkWriteUtil`                     | spark            | `copyOnWriteRequirements()`, `positionDeltaRequirements()` |
| **Row lineage**       | `RewriteUpdateTableForRowLineage`    | spark-extensions | Injects row lineage into UPDATE                            |
|                       | `RewriteMergeIntoTableForRowLineage` | spark-extensions | Injects row lineage into MERGE                             |
| **CoW commit API**    | `OverwriteFiles`                     | api              | `deleteFile()`, `addFile()`, `validateNoConflictingData()` |
| **MoR commit API**    | `RowDelta`                           | api              | `addRows()`, `addDeletes()`, `validateDeletedFiles()`      |
| **Isolation**         | `IsolationLevel`                     | core             | `SERIALIZABLE`, `SNAPSHOT`                                 |
| **Config**            | `TableProperties`                    | core             | `WRITE_UPDATE_MODE`, `WRITE_MERGE_MODE`, isolation levels  |
| **Write config**      | `SparkWriteConf`                     | spark            | `updateMode()`, `mergeMode()`, `updateIsolationLevel()`    |
