# Iceberg Delete Mechanisms — Position Deletes, Equality Deletes, and Deletion Vectors

This document provides a comprehensive description of how Apache Iceberg handles row-level deletes across format versions V2 and V3, covering Position Deletes, Equality Deletes, and Deletion Vectors (DVs).

**Related docs:** [architecture_overview.md](architecture_overview.md) | [read_path_callstack.md](read_path_callstack.md) | [write_path_callstack.md](write_path_callstack.md) | [compaction_callstack.md](compaction_callstack.md)

---

## 1. Overview — The Iceberg Delete Model

Iceberg data files are **immutable** — once written, they are never modified in place. To support SQL operations that logically remove or update rows (DELETE, UPDATE, MERGE INTO), Iceberg uses two strategies:

- **Copy-on-Write (CoW):** Rewrite entire data files with the affected rows removed. No delete files are produced — the old data file is replaced by a new one in the next snapshot.
- **Merge-on-Read (MoR):** Write lightweight **delete files** that record which rows are logically deleted. The deletes are applied at read time by filtering out deleted rows.

The MoR approach is faster for writes (avoids full file rewrites) but adds read overhead. Iceberg supports three types of delete files:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    ICEBERG DELETE FILE TYPES                               │
│                                                                            │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌────────────────────┐  │
│  │  Position Deletes   │  │  Equality Deletes   │  │ Deletion Vectors   │  │
│  │  (V2+)              │  │  (V2+)              │  │ (V3+)              │  │
│  │                     │  │                     │  │                    │  │
│  │  Parquet file with  │  │  Parquet file with  │  │  Puffin file with  │  │
│  │  file_path + pos    │  │  data rows matching │  │  RoaringBitmap     │  │
│  │  columns            │  │  equality fields    │  │  per data file     │  │
│  │                     │  │                     │  │                    │  │
│  │  Scope: per file    │  │  Scope: partition   │  │  Scope: per file   │  │
│  │  Apply: O(1) bitmap │  │  Apply: hash lookup │  │  Apply: O(1) bitmap│  │
│  └─────────────────────┘  └─────────────────────┘  └────────────────────┘  │
│                                                                            │
│  Format Version:  V1 = no deletes (append-only)                            │
│                   V2 = position deletes + equality deletes                 │
│                   V3 = adds deletion vectors (Puffin format)               │
└────────────────────────────────────────────────────────────────────────────┘
```

### Comparison Table

| Property                    | Position Deletes                      | Equality Deletes                                     | Deletion Vectors                                            |
|-----------------------------|---------------------------------------|------------------------------------------------------|-------------------------------------------------------------|
| **Format version**          | V2+                                   | V2+                                                  | V3+                                                         |
| **File format**             | Parquet                               | Parquet                                              | Puffin (binary blob)                                        |
| **Content type**            | `POSITION_DELETES`                    | `EQUALITY_DELETES`                                   | `POSITION_DELETES`                                          |
| **Scope**                   | Single data file (by `file_path`)     | All files in partition matching equality fields      | Single data file (by `referencedDataFile`)                  |
| **Columns stored**          | `file_path` (string) + `pos` (long)   | All equality field columns (full rows)               | Serialized RoaringBitmap of positions                       |
| **Read-time cost**          | Low — bitmap lookup O(1) per row      | High — hash set lookup per row, loaded for all files | Lowest — compact bitmap, direct offset access               |
| **Write-time cost**         | Low — record (path, pos) pairs        | Low — record matching rows                           | Low — set bits in bitmap                                    |
| **Storage overhead**        | Moderate — one record per deleted row | High — full row per deleted row                      | Low — compressed bitmap                                     |
| **Metadata fields**         | `recordCount`, `fileSizeInBytes`      | `equalityFieldIds[]`, metrics                        | `referencedDataFile`, `contentOffset`, `contentSizeInBytes` |
| **Multiple per data file?** | Yes (across multiple delete files)    | N/A (partition-scoped)                               | No (at most one DV per data file)                           |
| **Practical usage**         | Primary V2 mechanism                  | Rarely used in practice                              | Preferred V3 mechanism                                      |

---

## 2. Copy-on-Write vs Merge-on-Read

### Copy-on-Write (CoW) — Default

```
BEFORE:                                 AFTER:
┌────────────────────┐                  ┌────────────────────┐
│  data-file-001     │                  │  data-file-001     │  (deleted from snapshot)
│  row A  ◄── DELETE │   ══════════►    │                    │
│  row B             │                  └────────────────────┘
│  row C             │                  ┌────────────────────┐
└────────────────────┘                  │  data-file-002     │  (new file, rewritten)
                                        │  row B             │
                                        │  row C             │
                                        └────────────────────┘

Snapshot: RewriteFiles (delete old file, add new file)
No delete files produced.
```

- **Write cost:** High — must read + rewrite entire data file(s) containing affected rows
- **Read cost:** Zero — no delete files to apply at read time
- **Best for:** Read-heavy workloads, infrequent updates

### Merge-on-Read (MoR)

```
BEFORE:                                 AFTER:
┌────────────────────┐                  ┌────────────────────┐
│  data-file-001     │                  │  data-file-001     │  (unchanged)
│  row A  ◄── DELETE │   ══════════►    │  row A             │
│  row B             │                  │  row B             │
│  row C             │                  │  row C             │
└────────────────────┘                  └────────────────────┘
                                        ┌────────────────────┐
                                        │  delete-file-001   │  (new delete file)
                                        │  (data-file-001,   │
                                        │   pos=0)  → row A  │
                                        └────────────────────┘

Snapshot: RowDelta (add delete file referencing data-file-001)
Data file untouched. Delete applied at read time.
```

- **Write cost:** Low — write only a small delete file
- **Read cost:** Moderate — must load and apply delete file during scan
- **Best for:** Write-heavy workloads, frequent updates, near-real-time ingestion

### Configuration

| Property                       | Default         | Values                           | Scope                     |
|--------------------------------|-----------------|----------------------------------|---------------------------|
| `write.delete.mode`            | `copy-on-write` | `copy-on-write`, `merge-on-read` | DELETE statements         |
| `write.update.mode`            | `copy-on-write` | `copy-on-write`, `merge-on-read` | UPDATE statements         |
| `write.merge.mode`             | `copy-on-write` | `copy-on-write`, `merge-on-read` | MERGE INTO statements     |
| `write.delete.isolation-level` | `serializable`  | `serializable`, `snapshot`       | DELETE conflict detection |
| `write.update.isolation-level` | `serializable`  | `serializable`, `snapshot`       | UPDATE conflict detection |
| `write.merge.isolation-level`  | `serializable`  | `serializable`, `snapshot`       | MERGE conflict detection  |

These are **table properties** set via `ALTER TABLE ... SET TBLPROPERTIES(...)`.

### Mode Selection Flow (Spark)

```
SQL: DELETE FROM / UPDATE / MERGE INTO
         │
         ▼
SparkTable.newRowLevelOperationBuilder()                     [spark/source/SparkTable]
         │
         ▼
SparkRowLevelOperationBuilder                                [spark/source]
  │
  ├─► Read table property for command type:
  │     DELETE → write.delete.mode
  │     UPDATE → write.update.mode
  │     MERGE  → write.merge.mode
  │
  ├─► if mode == COPY_ON_WRITE:
  │     └─► return SparkCopyOnWriteOperation
  │           └─► Rewrites affected data files entirely
  │               (no delete files produced)
  │
  └─► if mode == MERGE_ON_READ:
        └─► return SparkPositionDeltaOperation
              └─► Writes position delete files or DVs
                  (data files left untouched)
```

---

## 3. Position Deletes (V2)

Position deletes identify rows to remove by their **physical location**: the data file path and the row's ordinal position (0-based) within that file.

### File Format

```
Position Delete File (Parquet):
┌───────────────────────────────────────────────┐
│  Schema:                                      │
│    file_path: string (required)               │  ◄── MetadataColumns.DELETE_FILE_PATH
│    pos:       long   (required)               │  ◄── MetadataColumns.DELETE_FILE_POS
│    row:       struct (optional)               │  ◄── for change data capture
│                                               │
│  Row 0: ("s3://bucket/data-001.parquet", 42)  │
│  Row 1: ("s3://bucket/data-001.parquet", 108) │
│  Row 2: ("s3://bucket/data-003.parquet", 7)   │
│  ...                                          │
│                                               │
│  Sorted by: (file_path ASC, pos ASC)          │  ◄── required for efficient merge
└───────────────────────────────────────────────┘
```

### Write Path

```
SQL: DELETE FROM table WHERE id = 42  (mode = merge-on-read)
        │
        ▼
SparkPositionDeltaOperation                                  [spark/source]
  │
  └─► SparkPositionDeltaWrite                                [spark/source/SparkPositionDeltaWrite]
        │
        └─► PositionDeltaBatchWrite.createBatchWriterFactory()
              │
              └─► Broadcast WriterFactory to executors

EXECUTOR:
  │
  ├─► Scan phase identifies affected rows:
  │     file_path = "data-001.parquet", pos = 42
  │     file_path = "data-001.parquet", pos = 108
  │
  ├─► BaseDeltaWriter variants:
  │     ├─► DeleteOnlyDeltaWriter      (DELETE only — no new data rows)
  │     ├─► UnpartitionedDeltaWriter   (UPDATE/MERGE unpartitioned)
  │     └─► PartitionedDeltaWriter     (UPDATE/MERGE partitioned)
  │
  └─► Delete writer selection (V2 tables):
        │
        ├─► if input ordered by (file, pos):
        │     ClusteredPositionDeleteWriter                  [core/io]
        │       └─► wraps PositionDeleteWriter               [core/deletes]
        │             └─► writes Parquet file with (file_path, pos) records
        │             └─► tracks referencedDataFiles (CharSequenceSet)
        │
        └─► if input unordered:
              FanoutPositionOnlyDeleteWriter                 [core/io]
                └─► routes records to per-file PositionDeleteWriter

PositionDeleteWriter                                         [core/deletes/PositionDeleteWriter]
  │
  ├─► for each PositionDelete<R>:
  │     appender.add(positionDelete)                         write (path, pos) to Parquet
  │     referencedDataFiles.add(positionDelete.path())       track which data files
  │
  └─► close():
        └─► DeleteFile result:
              ├─ content = POSITION_DELETES
              ├─ path = delete file location
              ├─ format = PARQUET
              ├─ recordCount = number of deleted positions
              ├─ fileSizeInBytes
              └─ partition

RollingPositionDeleteWriter                                  [core/io]
  └─► Splits into multiple files when target file size exceeded
```

### Data Structure

```java
// core/src/main/java/org/apache/iceberg/deletes/PositionDelete.java
public class PositionDelete<R> implements StructLike {
    private CharSequence path;     // Data file path containing the deleted row
    private long pos;              // 0-based ordinal position within the file
    private R row;                 // Optional: actual row data (for CDC/changelog)
}
```

### Read Path — How Position Deletes Are Applied

```
Planning (Driver):
  ManifestGroup.planFiles()
    └─► DeleteFileIndex.forDataFile(seqNum, dataFile)
          │
          ├─► Find position deletes scoped to this data file's path
          ├─► Filter by sequence number: delete.seqNum > data.seqNum
          └─► Return DeleteFile[] associated with this data file

Execution (Per-Task on Executors):
  DeleteFilter.filter(records)                               [data/DeleteFilter]
    │
    ├─► 1. Load position deletes:
    │     deleteLoader.loadPositionDeletes(posDeleteFiles, filePath)
    │       │
    │       ├─► Read position delete Parquet files
    │       ├─► Filter records where file_path == current data file
    │       └─► Build BitmapPositionDeleteIndex:
    │             RoaringPositionBitmap.set(pos)              for each deleted position
    │
    ├─► 2. Apply position delete filter:
    │     applyPosDeletes(records)
    │       │
    │       ├─► for each record:
    │       │     long pos = record.getAs(ROW_POSITION)
    │       │     if positionIndex.isDeleted(pos):            O(1) bitmap lookup
    │       │       skip record (deleted)
    │       │     else:
    │       │       emit record (not deleted)
    │       │
    │       └─► return filtered iterator
    │
    └─► 3. Apply equality deletes (if any — see section 4)
```

### Bitmap Index Implementation

```
BitmapPositionDeleteIndex                                    [core/deletes]
  │
  └─► Uses RoaringPositionBitmap:
        │
        ├─► 64-bit position split:
        │     upper 32 bits → key (selects which RoaringBitmap)
        │     lower 32 bits → position within that bitmap
        │
        ├─► Array of RoaringBitmap (one per key)
        │     supports billions of positions efficiently
        │
        ├─► Operations:
        │     set(long pos)              mark position as deleted
        │     setRange(start, end)       mark range as deleted
        │     contains(long pos)         check if position is deleted → O(1)
        │
        └─► Serialization:
              4-byte length + 4-byte magic + Roaring portable format + 4-byte CRC-32
```

---

## 4. Equality Deletes (V2)

Equality deletes identify rows to remove by **matching column values**. Instead of specifying a physical position, they contain the values of "equality columns" — any data row matching those column values is logically deleted.

### File Format

```
Equality Delete File (Parquet):
┌─────────────────────────────────────────┐
│  Schema:                                │
│    Same as table schema                 │
│    (all equality field columns present) │
│                                         │
│  Metadata:                              │
│    equalityFieldIds = [3, 7]            │  ◄── field IDs used for matching
│                                         │
│  Row 0: {id=42, name="Alice"}           │  ◄── delete all rows where
│  Row 1: {id=99, name="Bob"}             │      id=42 AND name="Alice", etc.
│  ...                                    │
│                                         │
│  Additional columns may be present      │
│  for metrics/bounds (not used for       │
│  equality matching)                     │
└─────────────────────────────────────────┘
```

### Key Characteristics

- **Partition-scoped:** An equality delete file in partition P applies to ALL data files in partition P whose data sequence number is less than the delete's sequence number
- **No file_path column:** Unlike position deletes, equality deletes do not reference a specific data file
- **Broad impact:** A single equality delete file can affect many data files — every file in the partition must be checked
- **Full row stored:** The delete file contains the full row data for the equality columns, not just positions

### Write Path

```
EqualityDeleteWriter                                         [core/deletes/EqualityDeleteWriter]
  │
  ├─► Constructor takes:
  │     int[] equalityFieldIds         which columns determine equality
  │     FileAppender<T> appender       writes rows to Parquet
  │     Schema eqDeleteRowSchema       schema of the rows being written
  │
  ├─► write(T row):
  │     appender.add(row)              write full row to Parquet
  │
  └─► close():
        └─► DeleteFile result:
              ├─ content = EQUALITY_DELETES
              ├─ equalityFieldIds = [3, 7, ...]
              ├─ format = PARQUET
              ├─ recordCount = number of delete rows
              ├─ metrics (column stats, bounds)
              └─ sortOrderId (if sorted)
```

### Read Path — How Equality Deletes Are Applied

```
Planning (Driver):
  DeleteFileIndex.forDataFile(seqNum, dataFile)
    │
    ├─► Find equality deletes in same partition
    ├─► Filter by sequence number
    └─► Group by equalityFieldIds

Execution (Per-Task on Executors):
  DeleteFilter.applyEqDeletes(records)                       [data/DeleteFilter]
    │
    ├─► For each group of equality deletes (same equalityFieldIds):
    │     │
    │     ├─► Project delete schema to equality columns only
    │     │
    │     ├─► Load all delete rows into StructLikeSet:
    │     │     deleteSet = deleteLoader.loadEqualityDeletes(deleteFiles, deleteSchema)
    │     │       └─► Read all Parquet delete files
    │     │           Project to equality columns
    │     │           Add each row to hash set
    │     │
    │     └─► Create predicate:
    │           isDeleted = record →
    │             deleteSet.contains(
    │               projectRow.wrap(asStructLike(record)))     hash lookup
    │
    ├─► Combine predicates with OR:
    │     isEqDeleted = pred1.or(pred2).or(...)
    │
    └─► Filter records:
          for each record:
            if isEqDeleted(record): skip
            else: emit
```

### Schema Expansion

Equality deletes require the equality field columns to be present in the read projection, even if the user's query didn't select them:

```
User query:   SELECT name FROM table WHERE age > 30
Table has equality delete with equalityFieldIds = [1]  (field 1 = "id")

Actual read projection:
  ┌──────┬──────┐
  │ name │  id  │  ◄── "id" added for equality delete evaluation
  └──────┴──────┘       (stripped from output after filtering)
```

### Performance Implications

- **Memory:** All equality delete rows loaded into memory as `StructLikeSet`
- **CPU:** Hash set membership check per data row, per equality delete group
- **I/O:** Must read all equality delete files for the partition
- **Blast radius:** Every data file in the partition must be scanned against the delete set
- **Practical usage:** Rarely used in production. Position deletes (or DVs) are strongly preferred because they are file-scoped and cheaper to apply

---

## 5. Deletion Vectors (V3)

Deletion Vectors (DVs) are the V3 evolution of position deletes. They use a compact **RoaringBitmap** serialized into a **Puffin file** instead of row-by-row Parquet records.

### File Format

```
Puffin File (deletion-vectors.puffin):
┌──────────────────────────────────────────────────────────────────┐
│  Magic: 0x50 0x46 0x41 0x31  ("PFA1")                            │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ Blob 0:  DV for data-file-001.parquet                   │     │
│  │                                                         │     │
│  │  Type: "apache-datasketches-theta-v1" (DV_V1)           │     │
│  │  Metadata:                                              │     │
│  │    referenced-data-file: "data-file-001.parquet"        │     │
│  │    cardinality: 3                                       │     │
│  │  Payload: serialized RoaringBitmap                      │     │
│  │    [positions: 42, 108, 255]                            │     │
│  └─────────────────────────────────────────────────────────┘     │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ Blob 1:  DV for data-file-003.parquet                   │     │
│  │                                                         │     │
│  │  Type: "apache-datasketches-theta-v1" (DV_V1)           │     │
│  │  Metadata:                                              │     │
│  │    referenced-data-file: "data-file-003.parquet"        │     │
│  │    cardinality: 1                                       │     │
│  │  Payload: serialized RoaringBitmap                      │     │
│  │    [positions: 7]                                       │     │
│  └─────────────────────────────────────────────────────────┘     │
│                                                                  │
│  Footer:                                                         │
│    Blob metadata (JSON): types, offsets, lengths                 │
│    Footer length (4 bytes)                                       │
│    Flags (4 bytes)                                               │
│    Magic: 0x50 0x46 0x41 0x31                                    │
└──────────────────────────────────────────────────────────────────┘
```

### Key Differences from Position Deletes

| Aspect         | Position Deletes (V2)                           | Deletion Vectors (V3)                                       |
|----------------|-------------------------------------------------|-------------------------------------------------------------|
| File format    | Parquet (row-based)                             | Puffin (binary blob)                                        |
| Storage        | One record per deleted row                      | Compressed bitmap — orders of magnitude smaller             |
| Access pattern | Sequential scan of delete file                  | Direct offset/length access to blob                         |
| Per data file  | Multiple delete files possible                  | At most ONE DV per data file                                |
| Metadata       | Just `recordCount`                              | `referencedDataFile`, `contentOffset`, `contentSizeInBytes` |
| Mergeability   | Must read all delete files and union            | Can load bitmap, set new bits, rewrite                      |
| Loading cost   | Read Parquet, filter by file_path, build bitmap | Read blob at offset, deserialize bitmap directly            |

### Write Path

```
SQL: DELETE FROM table WHERE id = 42  (mode = merge-on-read, V3 table)
        │
        ▼
SparkPositionDeltaWrite.newDeleteWriter()                    [spark/source]
  │
  ├─► context.useDVs() == true   (V3 table)
  │
  └─► new PartitioningDVWriter(files, previousDeleteLoader)

PartitioningDVWriter                                         [core/io]
  │
  └─► delegates to BaseDVFileWriter                          [core/deletes/BaseDVFileWriter]

BaseDVFileWriter                                             [core/deletes/BaseDVFileWriter]
  │
  ├─► ACCUMULATION PHASE:
  │     │
  │     ├─► delete(String path, long pos, PartitionSpec spec, StructLike partition)
  │     │     │
  │     │     └─► Map<String, Deletes> dvs:
  │     │           key = data file path
  │     │           value = Deletes {
  │     │             path: data file path,
  │     │             positions: BitmapPositionDeleteIndex,  ◄── RoaringBitmap
  │     │             spec: partition spec,
  │     │             partition: partition values
  │     │           }
  │     │
  │     └─► positions.delete(pos)                            set bit in bitmap
  │
  ├─► MERGE WITH PREVIOUS (optional):
  │     │
  │     └─► If previous DV exists for this data file:
  │           previousDeleteLoader.loadPositionDeletes(oldDV, path)
  │             → oldBitmap
  │           newBitmap = merge(oldBitmap, currentBitmap)    union of positions
  │
  └─► CLOSE (write Puffin file):
        │
        ├─► PuffinWriter puffinWriter = Puffin.write(outputFile)
        │
        ├─► For each data file in dvs:
        │     │
        │     ├─► Serialize RoaringPositionBitmap to bytes:
        │     │     bitmap.runLengthOptimize()                compact before serialization
        │     │     bytes = bitmap.serialize()
        │     │
        │     ├─► Write blob to Puffin:
        │     │     puffinWriter.add(
        │     │       type = StandardBlobTypes.DV_V1,
        │     │       fieldIds = [ROW_POSITION],
        │     │       metadata = {
        │     │         "referenced-data-file": path,
        │     │         "cardinality": bitmap.cardinality()
        │     │       },
        │     │       payload = bytes)
        │     │
        │     └─► Record blob position:
        │           contentOffset = blob start in file
        │           contentSizeInBytes = blob length
        │
        ├─► puffinWriter.close()
        │
        └─► Create one DeleteFile per data file:
              FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()                         ◄── still POSITION_DELETES content type
                .withFormat(FileFormat.PUFFIN)
                .withPath(puffinFilePath)
                .withReferencedDataFile(dataFilePath)        ◄── single data file reference
                .withContentOffset(blobOffset)               ◄── direct access to blob
                .withContentSizeInBytes(blobSize)
                .withRecordCount(bitmap.cardinality())
                .build()
```

### Read Path

```
Planning (Driver):
  DeleteFileIndex.forDataFile(seqNum, dataFile)
    │
    ├─► Look for DV referencing this data file:
    │     dv = pathToDV.get(dataFile.path())
    │
    └─► DV takes priority:
          if dv != null && no other deletes:
            return [dv]
          if dv != null && has equality deletes:
            return concat(equalityDeletes, [dv])

Execution (Executors):
  BaseDeleteLoader.loadPositionDeletes(dvDeleteFile, filePath)
    │
    ├─► Detect DV: deleteFile.referencedDataFile() != null
    │
    ├─► Read blob directly from Puffin:
    │     inputFile.newStream()
    │       .seek(deleteFile.contentOffset())
    │       .read(deleteFile.contentSizeInBytes())
    │
    ├─► Deserialize:
    │     BitmapPositionDeleteIndex.deserialize(blobBytes)
    │       → RoaringPositionBitmap
    │
    └─► Same interface as position deletes:
          positionIndex.isDeleted(pos) → true/false          O(1) per row
```

### Write Decision Logic

```
SparkPositionDeltaWrite.newDeleteWriter()                    [spark/source]
  │
  ├─► if context.useDVs():                                  V3+ table
  │     └─► PartitioningDVWriter
  │           └─► BaseDVFileWriter → Puffin file
  │
  ├─► elif inputOrdered && no rewritable deletes:            V2, ordered
  │     └─► ClusteredPositionDeleteWriter
  │           └─► PositionDeleteWriter → Parquet file
  │
  └─► else:                                                  V2, unordered
        └─► FanoutPositionOnlyDeleteWriter
              └─► per-file PositionDeleteWriter → Parquet files
```

---

## 6. Delete Application During Reads — Unified Flow

### Planning Phase (Driver)

```
DataTableScan.planFiles()                                    [core/DataTableScan]
  │
  └─► ManifestGroup.planFiles()                              [core/ManifestGroup]
        │
        ├─► Build DeleteFileIndex from all delete manifests:
        │     DeleteFileIndex.build(table, snapshot)          [core/DeleteFileIndex]
        │       │
        │       ├─► Read all delete manifest files
        │       ├─► Index position deletes by partition
        │       ├─► Index equality deletes by partition + field IDs
        │       ├─► Index DVs by referenced data file path
        │       └─► Store global equality deletes separately
        │
        └─► For each data manifest entry:
              │
              ├─► deleteFiles = DeleteFileIndex.forDataFile(
              │     entry.dataSequenceNumber(),
              │     entry.file())
              │       │
              │       ├─► Sequence number filtering:
              │       │     include delete only if
              │       │     delete.dataSequenceNumber > data.dataSequenceNumber
              │       │
              │       ├─► For position deletes: match by partition
              │       ├─► For equality deletes: match by partition + field scope
              │       ├─► For DVs: match by referencedDataFile path
              │       │
              │       └─► DV precedence:
              │             if DV exists for this file, include it
              │             (DV is canonical source of positional deletes)
              │
              └─► yield FileScanTask(
                    file = dataFile,
                    deletes = deleteFiles[],     ◄── associated delete files
                    residual = residualFilter,
                    spec = partitionSpec)
```

### Sequence Number Rule

```
Timeline:

  Snapshot 1 (seq=1):  data-file-A added
  Snapshot 2 (seq=2):  data-file-B added
  Snapshot 3 (seq=3):  delete-file-X added (deletes from data-file-A)
  Snapshot 4 (seq=4):  data-file-C added

  delete-file-X.dataSequenceNumber = 3

  Applies to data-file-A?  YES  (3 > 1)
  Applies to data-file-B?  YES  (3 > 2)
  Applies to data-file-C?  NO   (3 < 4)  ◄── data added AFTER the delete

  This prevents retroactive application of deletes to newer data.
```

### Execution Phase (Executors)

```
DeleteFilter.filter(CloseableIterable<T> records)            [data/DeleteFilter]
  │
  │  DELETE APPLICATION ORDER:
  │  Position deletes first, then equality deletes
  │
  ├─► Step 1: Apply position deletes
  │     applyPosDeletes(records)
  │       │
  │       ├─► Load position index:
  │       │     PositionDeleteIndex posIndex = deletedRowPositions()
  │       │       └─► deleteLoader.loadPositionDeletes(posDeletes, filePath)
  │       │             │
  │       │             ├─► For Parquet position deletes:
  │       │             │     Read Parquet file
  │       │             │     Filter: file_path == current data file path
  │       │             │     For each (file_path, pos): bitmap.set(pos)
  │       │             │
  │       │             └─► For DVs (Puffin):
  │       │                   Read blob at contentOffset
  │       │                   Deserialize RoaringPositionBitmap
  │       │
  │       └─► Filter:
  │             isDeleted = record → posIndex.isDeleted(pos(record))
  │             return filterDeleted(records, isDeleted)
  │
  ├─► Step 2: Apply equality deletes (if any)
  │     applyEqDeletes(posFiltered)
  │       │
  │       ├─► For each equality delete group (same field IDs):
  │       │     StructLikeSet deleteSet = loadEqualityDeletes(files, schema)
  │       │     isEqDeleted = record → deleteSet.contains(project(record))
  │       │
  │       └─► Filter:
  │             combinedPredicate = allPredicates.reduce(OR)
  │             return filterDeleted(posFiltered, combinedPredicate)
  │
  └─► Return: fully filtered record stream

  Two modes of operation:
  ├─► Standard mode (no _is_deleted column):
  │     Deletes.filterDeleted(records, predicate)
  │     → Actually removes rows from output
  │
  └─► Marking mode (with _is_deleted metadata column):
        Deletes.markDeleted(records, predicate, markRowDeleted)
        → Sets _is_deleted=true but keeps rows in output (for CDC/changelogs)
```

---

## 7. SQL Operation -> Delete Type Matrix

```
┌───────────────┬────────────────────────┬─────────────────────────────────────────┐
│  SQL Command  │  Copy-on-Write (CoW)   │  Merge-on-Read (MoR)                    │
│               │  (write.*.mode=cow)    │  (write.*.mode=mor)                     │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  DELETE       │  Rewrite data files    │  V2: Position delete files (Parquet)    │
│  FROM ...     │  without deleted rows  │  V3: Deletion vectors (Puffin)          │
│  WHERE ...    │                        │                                         │
│               │  API: OverwriteFiles   │  API: RowDelta.addDeletes()             │
│               │  Snapshot op: replace  │  Snapshot op: overwrite                 │
│               │                        │                                         │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  UPDATE       │  Rewrite data files    │  Position delete (or DV) for old row    │
│  ...          │  with updated values   │  + new data file with updated row       │
│  SET ...      │                        │                                         │
│               │  (delete + insert      │  (delete + insert, two file types)      │
│               │   within same file)    │                                         │
│               │                        │  API: RowDelta.addDeletes() +           │
│               │  API: OverwriteFiles   │       RowDelta.addRows()                │
│               │                        │                                         │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  MERGE INTO   │  Rewrite data files    │  Position delete (or DV) for matched    │
│  ...          │  for matched rows      │  rows that are updated/deleted          │
│  WHEN MATCHED │  + new files for       │  + new data files for inserted/updated  │
│  WHEN NOT     │    inserted rows       │    rows                                 │
│  MATCHED      │                        │                                         │
│               │  API: OverwriteFiles   │  API: RowDelta                          │
│               │  + AppendFiles         │                                         │
│               │                        │                                         │
├───────────────┼────────────────────────┼─────────────────────────────────────────┤
│               │                        │                                         │
│  INSERT INTO  │  (no deletes — append  │  (no deletes — append only)             │
│               │   only)                │                                         │
│               │                        │                                         │
│               │  API: AppendFiles      │  API: AppendFiles                       │
│               │                        │                                         │
└───────────────┴────────────────────────┴─────────────────────────────────────────┘
```

### MoR: How UPDATE Works as Delete + Insert

```
UPDATE table SET name = 'Bob' WHERE id = 42

MoR execution flow:

1. SCAN: Find rows where id = 42
   → data-file-001.parquet, position 17:  {id=42, name="Alice"}

2. DELETE (position delete / DV):
   → delete-file (data-file-001.parquet, pos=17)

3. INSERT (new data file):
   → data-file-002.parquet, row 0:  {id=42, name="Bob"}

4. COMMIT:
   RowDelta
     .addDeletes(deleteFile)       position delete or DV
     .addRows(dataFile002)         new data file with updated row
     .commit()
```

---

## 8. RowDelta API

The `RowDelta` API is the primary interface for committing MoR changes (data files + delete files in a single atomic operation).

```
RowDelta API                                                 [api/RowDelta]
  │
  ├─► addRows(DataFile inserts)
  │     Add new data files (inserts or updated rows)
  │
  ├─► addDeletes(DeleteFile deletes)
  │     Add position delete files, equality delete files, or DVs
  │
  ├─► removeRows(DataFile file)
  │     Remove a data file (for rewrite operations)
  │
  ├─► removeDeletes(DeleteFile deletes)
  │     Remove old delete files (when rewriting/merging DVs)
  │
  ├─► validateFromSnapshot(long snapshotId)
  │     Set the baseline snapshot for conflict detection
  │
  ├─► validateDataFilesExist(Iterable<CharSequence> referencedFiles)
  │     Ensure data files referenced by position deletes still exist
  │     (prevents dangling delete references)
  │
  ├─► validateNoConflictingDataFiles()
  │     Detect concurrent data modifications (serializable isolation)
  │
  ├─► validateNoConflictingDeleteFiles()
  │     Detect concurrent delete modifications
  │     (required for UPDATE and MERGE to prevent lost updates)
  │
  └─► commit()
        └─► BaseRowDelta (MergingSnapshotProducer)           [core/BaseRowDelta]
              │
              ├─► Validate all constraints
              ├─► Write new manifest with added/removed files
              ├─► Write manifest list
              ├─► Create snapshot (operation = "overwrite")
              └─► TableOperations.commit() → atomic CAS
```

### Isolation Levels

| Level          | Behavior                                                                                    | Use Case                                                              |
|----------------|---------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| `serializable` | Validates no concurrent data OR delete changes in affected partitions. Retries on conflict. | Safe default for correctness                                          |
| `snapshot`     | Only validates no concurrent delete changes. Allows concurrent inserts.                     | Higher throughput when concurrent inserts are expected and acceptable |

---

## 9. Compaction and Delete Cleanup

Over time, MoR tables accumulate delete files that degrade read performance. Compaction and maintenance operations clean these up.

### Compaction Applies Deletes

```
RewriteDataFiles (compaction)
  │
  ├─► Read data files WITH their associated delete files
  │     → deletes applied during read (rows filtered out)
  │
  ├─► Write new data files (without deleted rows)
  │
  └─► Commit: RewriteFiles
        .deleteFile(old data file)
        .deleteFile(old delete file)      ◄── delete files removed too
        .addFile(new data file)           ◄── clean file, no pending deletes
        .commit()

Result: delete files consumed and removed, data files compacted
```

### Compaction Triggers Based on Deletes

| Property                 | Default   | Purpose                                            |
|--------------------------|-----------|----------------------------------------------------|
| `delete-file-threshold`  | `MAX_INT` | Rewrite data files associated with N+ delete files |
| `delete-ratio-threshold` | `0.3`     | Rewrite data files where 30%+ of rows are deleted  |

Example:
```sql
CALL catalog.system.rewrite_data_files(
  table => 'db.table',
  options => map(
    'delete-file-threshold', '3',    -- rewrite if 3+ delete files
    'delete-ratio-threshold', '0.1'  -- rewrite if 10%+ rows deleted
  )
)
```

### Delete File Lifecycle

```
Time ──────────────────────────────────────────────────────────►

1. DELETE WHERE id=42 (MoR)
   → delete-file-001 created (references data-file-A)

2. DELETE WHERE id=99 (MoR)
   → delete-file-002 created (references data-file-A)

3. Compaction runs:
   → Read data-file-A, apply delete-file-001 + delete-file-002
   → Write data-file-B (clean, no deleted rows)
   → Commit: remove data-file-A + delete-file-{001,002}, add data-file-B

4. ExpireSnapshots:
   → Old snapshots referencing delete-file-{001,002} expired
   → Files now orphaned (no snapshot references them)

5. RemoveOrphanFiles (or GC):
   → delete-file-{001,002} physically deleted from storage
```

### Related Maintenance Operations

| Operation                    | Purpose                                                      | API                                                            |
|------------------------------|--------------------------------------------------------------|----------------------------------------------------------------|
| `RewriteDataFiles`           | Compact data files, applying pending deletes                 | `SparkActions.rewriteDataFiles()`                              |
| `RemoveDanglingDeletes`      | Remove delete files that no longer reference live data       | `rewriteDataFiles().option("remove-dangling-deletes", "true")` |
| `ExpireSnapshots`            | Remove old snapshots, enabling GC of unreferenced files      | `SparkActions.expireSnapshots()`                               |
| `RemoveOrphanFiles`          | Delete files not referenced by any snapshot                  | `SparkActions.removeOrphanFiles()`                             |
| `RewritePositionDeleteFiles` | Rewrite fragmented position delete files for better locality | `SparkActions.rewritePositionDeletes()`                        |

---

## 10. Key Classes Reference

| Area                        | Class                            | Module       | Key Method                                                          |
|-----------------------------|----------------------------------|--------------|---------------------------------------------------------------------|
| **Delete types**            | `DeleteFile`                     | api          | Interface for all delete files                                      |
|                             | `FileContent`                    | api          | Enum: `DATA`, `POSITION_DELETES`, `EQUALITY_DELETES`                |
|                             | `PositionDelete<R>`              | core/deletes | `set(path, pos)`, `set(path, pos, row)`                             |
| **Position delete writing** | `PositionDeleteWriter`           | core/deletes | `write(PositionDelete)`, `close()`                                  |
|                             | `RollingPositionDeleteWriter`    | core/io      | Splits large delete files by size                                   |
|                             | `ClusteredPositionDeleteWriter`  | core/io      | Assumes ordered input                                               |
|                             | `FanoutPositionOnlyDeleteWriter` | core/io      | Per-file fanout routing                                             |
| **Equality delete writing** | `EqualityDeleteWriter`           | core/deletes | `write(T row)`, `close()`                                           |
|                             | `RollingEqualityDeleteWriter`    | core/io      | Splits large equality delete files                                  |
| **Deletion vector writing** | `DVFileWriter`                   | core/deletes | Interface for DV writers                                            |
|                             | `BaseDVFileWriter`               | core/deletes | `delete(path, pos, spec, partition)`, `close()`                     |
|                             | `PartitioningDVWriter`           | core/io      | Partitioned DV output                                               |
|                             | `PuffinWriter`                   | core/puffin  | Binary blob file format                                             |
| **Bitmap index**            | `PositionDeleteIndex`            | core/deletes | Interface: `isDeleted(long pos)`                                    |
|                             | `BitmapPositionDeleteIndex`      | core/deletes | RoaringBitmap implementation                                        |
|                             | `RoaringPositionBitmap`          | core/deletes | 64-bit Roaring bitmap                                               |
| **Delete filter (reads)**   | `DeleteFilter`                   | data         | `filter()`, `applyPosDeletes()`, `applyEqDeletes()`                 |
|                             | `BaseDeleteLoader`               | data         | `loadPositionDeletes()`, `loadEqualityDeletes()`                    |
|                             | `DeleteFileIndex`                | core         | `forDataFile()` — associates deletes with data files                |
|                             | `Deletes`                        | core/deletes | Utility: `filterDeleted()`, `markDeleted()`                         |
| **Spark delete reads**      | `PositionDeletesRowReader`       | spark/source | Reads position delete files as data                                 |
|                             | `EqualityDeleteRowReader`        | spark/source | Reads equality delete files as data                                 |
|                             | `DVIterator`                     | spark/source | Extracts positions from Puffin DV blobs                             |
| **Spark MoR write**         | `SparkPositionDeltaOperation`    | spark/source | MoR entry point                                                     |
|                             | `SparkPositionDeltaWrite`        | spark/source | MoR write orchestration                                             |
|                             | `BaseDeltaWriter`                | spark/source | Base class for MoR task writers                                     |
|                             | `DeleteOnlyDeltaWriter`          | spark/source | DELETE-only MoR writer                                              |
|                             | `UnpartitionedDeltaWriter`       | spark/source | UPDATE/MERGE unpartitioned                                          |
|                             | `PartitionedDeltaWriter`         | spark/source | UPDATE/MERGE partitioned                                            |
| **Spark CoW write**         | `SparkCopyOnWriteOperation`      | spark/source | CoW entry point                                                     |
| **Mode selection**          | `SparkRowLevelOperationBuilder`  | spark/source | CoW vs MoR decision                                                 |
|                             | `RowLevelOperationMode`          | core         | Enum: `COPY_ON_WRITE`, `MERGE_ON_READ`                              |
| **Commit API**              | `RowDelta`                       | api          | `addRows()`, `addDeletes()`, `commit()`                             |
|                             | `BaseRowDelta`                   | core         | Implementation of RowDelta                                          |
| **Configuration**           | `TableProperties`                | core         | `WRITE_DELETE_MODE`, `WRITE_UPDATE_MODE`, etc.                      |
| **Metadata**                | `MetadataColumns`                | core         | `DELETE_FILE_PATH`, `DELETE_FILE_POS`, `ROW_POSITION`, `IS_DELETED` |
