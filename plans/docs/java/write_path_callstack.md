# Iceberg Write Path — Call Stack

This document traces the complete write path from a Spark SQL INSERT to Parquet file writing and metadata commit.

---

## 1. High-Level Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│  SPARK SQL:  INSERT INTO catalog.db.table VALUES (...)               │
│              CREATE TABLE ... AS SELECT ...                          │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │  TABLE RESOLUTION     │   DRIVER
                    │                       │
                    │  SparkTable           │
                    │    .newWriteBuilder() │
                    │       │               │
                    │       ▼               │
                    │  SparkWriteBuilder    │
                    │    .build()           │
                    │       │               │
                    │       ▼               │
                    │  SparkWrite           │
                    │    .toBatch()         │
                    │       │               │
                    │       ▼               │
                    │  BatchAppend (or      │
                    │  DynamicOverwrite/    │
                    │  OverwriteByFilter)   │
                    └───────────┬───────────┘
                                │
                                │ createBatchWriterFactory()
                                │ → broadcast WriterFactory
                                │
         ┌──────────────────────┴─────────────────────────┐
         │                                                │
         ▼                                                ▼
┌────────────────────┐                     ┌────────────────────┐
│  EXECUTOR 1        │                     │  EXECUTOR N        │
│                    │                     │                    │
│  WriterFactory     │                     │  WriterFactory     │
│    .createWriter() │                     │    .createWriter() │
│       │            │                     │       │            │
│       ▼            │                     │       ▼            │
│  DataWriter        │                     │  DataWriter        │
│  (Unpartitioned    │                     │  (Partitioned      │
│   or Partitioned)  │                     │   or Unpartitioned)│
│       │            │                     │       │            │
│       ▼            │                     │       ▼            │
│  RollingDataWriter │                     │  FanoutDataWriter  │
│  / ClusteredWriter │                     │  / ClusteredWriter │
│       │            │                     │       │            │
│       ▼            │                     │       ▼            │
│  Parquet.writeData │                     │  Parquet.writeData │
│       │            │                     │       │            │
│       ▼            │                     │       ▼            │
│  ParquetWriter     │                     │  ParquetWriter     │
│    .add(row)       │                     │    .add(row)       │
│       │            │                     │       │            │
│       ▼            │                     │       ▼            │
│  .parquet files    │                     │  .parquet files    │
│                    │                     │                    │
│  commit() →        │                     │  commit() →        │
│  TaskCommit(files) │                     │  TaskCommit(files) │
└────────┬───────────┘                     └────────┬───────────┘
         │                                          │
         └────────────────┬─────────────────────────┘
                          │ WriterCommitMessage[]
                          ▼
              ┌───────────────────────┐
              │  METADATA COMMIT      │   DRIVER
              │                       │
              │  BatchAppend.commit() │
              │       │               │
              │       ▼               │
              │  AppendFiles          │
              │  (FastAppend)         │
              │    .appendFile(f)     │
              │    .commit()          │
              │       │               │
              │       ▼               │
              │  SnapshotProducer     │
              │    write manifests    │
              │    write manifest list│
              │    commit metadata    │
              │       │               │
              │       ▼               │
              │  NEW SNAPSHOT         │
              └───────────────────────┘
```

---

## 2. Detailed Call Stack

### Phase 1: Write Configuration (Driver)

```
SparkTable.newWriteBuilder(LogicalWriteInfo info)                [spark/source/SparkTable]
  └─► new SparkWriteBuilder(spark, table, branch, info)

SparkWriteBuilder                                                [spark/source/SparkWriteBuilder]
  implements: WriteBuilder, SupportsDynamicOverwrite, SupportsOverwrite
  │
  ├─► overwriteDynamicPartitions()                               dynamic partition overwrite
  ├─► overwrite(filters)                                         filter-based overwrite
  │
  └─► build()
        └─► new SparkWrite(spark, table, writeConf, info, ...)
              └─► toBatch()
                    ├─► BatchAppend (INSERT INTO)
                    ├─► DynamicOverwrite (INSERT OVERWRITE dynamic)
                    └─► OverwriteByFilter (INSERT OVERWRITE static)
```

### Phase 2: Writer Factory Creation (Driver)

```
BatchAppend.createBatchWriterFactory(PhysicalWriteInfo)          [spark/source/SparkWrite]
  │
  └─► createWriterFactory()                                      [SparkWrite]
        │
        ├─► new SparkFileWriterFactory(...)                      [spark/source/SparkFileWriterFactory]
        │     extends BaseFileWriterFactory<InternalRow>         [data/BaseFileWriterFactory]
        │     │
        │     └─► configures:
        │           table, fileFormat, schema, spec,
        │           io, encryptionManager, targetFileSize,
        │           writeProperties
        │
        └─► new WriterFactory(fileWriterFactory, fileFormat,
                              targetFileSize, writeSchema,
                              dsSchema, partitioned, fanout)
              │
              └─► serialized & broadcast to executors
```

### Phase 3: Per-Task Writing (Executors)

```
WriterFactory.createWriter(partitionId, taskId)                  [spark/source/SparkWrite]
  │
  ├─► if unpartitioned:
  │     └─► new UnpartitionedDataWriter(...)
  │           └─► wraps RollingDataWriter<InternalRow>
  │
  └─► if partitioned:
        └─► new PartitionedDataWriter(...)
              │
              ├─► if fanoutEnabled:
              │     └─► wraps FanoutDataWriter<InternalRow>
              │           (keeps one writer open per partition seen)
              │
              └─► else:
                    └─► wraps ClusteredDataWriter<InternalRow>
                          (expects rows sorted by partition)

DataWriter.write(InternalRow row)                                [spark/source/SparkWrite]
  │
  ├─► UnpartitionedDataWriter:
  │     └─► rollingWriter.write(row)
  │           ├─► if currentWriter == null || file >= targetSize:
  │           │     closeCurrentWriter()
  │           │     openNewWriter()                              (rolls to new file)
  │           └─► currentWriter.write(row)
  │
  └─► PartitionedDataWriter:
        └─► partitioningWriter.write(row, spec, partition)
              ├─► route to partition-specific writer
              └─► writer.write(row)
```

### Phase 4: Parquet File Writing (Executors)

```
SparkFileWriterFactory.newDataWriter(file, spec, partition)      [spark/source/SparkFileWriterFactory]
  │
  └─► configureDataWrite(Parquet.DataWriteBuilder)
        │
        └─► Parquet.writeData(encryptedOutputFile)               [parquet/Parquet]
              .schema(dataSchema)
              .setAll(writeProperties)
              .metricsConfig(metricsConfig)
              .withSpec(spec)
              .withPartition(partition)
              .withSortOrder(sortOrder)
              .overwrite()
              .createWriterFunc(msgType →
                SparkParquetWriters.buildWriter(sparkType, msgType))  [spark/data]
              .build()
                └─► returns DataWriter<InternalRow>
                      wrapping ParquetWriter<InternalRow>

ParquetWriter<InternalRow>                                       [parquet/ParquetWriter]
  │
  └─► add(InternalRow value)
        ├─► recordCount += 1
        ├─► model.write(0, value)                                ParquetValueWriter tree
        │     └─► SparkParquetWriters encode each column:
        │           StructWriter
        │             ├─► IntWriter.write(col0)
        │             ├─► StringWriter.write(col1)
        │             ├─► DecimalWriter.write(col2)
        │             └─► ...
        │
        ├─► writeStore.endRecord()                               buffer in page store
        │
        └─► checkSize()
              └─► if buffered >= targetRowGroupSize:
                    flushRowGroup()
                      └─► ParquetFileWriter.startBlock(rowCount)
                          columnChunkPageWriteStore.flushToWriter()
                          ParquetFileWriter.endBlock()

  close()
    ├─► flush remaining rows
    ├─► write Parquet footer (schema, row group metadata, stats)
    └─► collect metrics (record count, file size, column stats)
          └─► returns Metrics object
```

### Phase 5: Task Commit (Executors → Driver)

```
DataWriter.commit()                                              [spark/source/SparkWrite]
  │
  ├─► close underlying writers
  │
  ├─► collect DataFile[] from write results:
  │     DataFile
  │       ├─ filePath
  │       ├─ fileFormat (PARQUET)
  │       ├─ partition
  │       ├─ recordCount
  │       ├─ fileSizeInBytes
  │       ├─ columnSizes
  │       ├─ valueCounts
  │       ├─ nullValueCounts
  │       ├─ lowerBounds
  │       └─ upperBounds
  │
  └─► return TaskCommit(DataFile[])                              WriterCommitMessage
        └─► serialized, sent back to driver
```

### Phase 6: Metadata Commit (Driver)

```
BatchAppend.commit(WriterCommitMessage[] messages)               [spark/source/SparkWrite]
  │
  ├─► collect all DataFile[] from TaskCommit messages
  │
  ├─► AppendFiles append = table.newAppend()                     [api/Table]
  │     └─► new FastAppend(tableName, ops)                       [core/FastAppend]
  │
  ├─► for each DataFile:
  │     append.appendFile(dataFile)
  │
  └─► commitOperation(append, description)                       [SparkWrite]
        └─► append.commit()                                      [core/FastAppend]
              └─► SnapshotProducer.commit()                      [core/SnapshotProducer]

SnapshotProducer.commit()                                        [core/SnapshotProducer]
  │
  ├─► apply() → produce new Snapshot
  │     │
  │     ├─► write new manifest files:
  │     │     ManifestWriter.write(manifestEntries)
  │     │       └─► Avro file with DataFile entries
  │     │           (status: ADDED for new files)
  │     │
  │     ├─► write manifest list:
  │     │     ManifestListWriter.write(manifests)
  │     │       └─► snap-<snapshotId>-<attempt>.avro
  │     │
  │     └─► create new Snapshot:
  │           ├─ snapshotId (unique)
  │           ├─ parentId (previous snapshot)
  │           ├─ sequenceNumber (incremented)
  │           ├─ timestampMillis
  │           ├─ operation = "append"
  │           ├─ summary (added-data-files, added-records, etc.)
  │           └─ manifestListLocation
  │
  └─► TableOperations.commit(base, updated)
        │
        ├─► write new metadata.json:
        │     v<N+1>.metadata.json
        │       ├─ current-snapshot-id = new snapshot
        │       ├─ snapshots[] += new snapshot
        │       └─ snapshot-log[] += entry
        │
        ├─► atomic compare-and-swap on metadata pointer
        │     (implementation varies by catalog)
        │
        └─► on conflict: retry with exponential backoff
              └─► re-read base, re-apply changes, commit again
```

---

## 3. Write Variants

```
┌─────────────────────┬───────────────────────┬─────────────────────────┐
│  Operation          │  BatchWrite class     │  Iceberg SnapshotUpdate │
├─────────────────────┼───────────────────────┼─────────────────────────┤
│  INSERT INTO        │  BatchAppend          │  AppendFiles (FastAppend│
│                     │                       │  or MergeAppend)        │
├─────────────────────┼───────────────────────┼─────────────────────────┤
│  INSERT OVERWRITE   │  DynamicOverwrite     │  ReplacePartitions      │
│  (dynamic)          │                       │                         │
├─────────────────────┼───────────────────────┼─────────────────────────┤
│  INSERT OVERWRITE   │  OverwriteByFilter    │  OverwriteFiles         │
│  (static)           │                       │                         │
├─────────────────────┼───────────────────────┼─────────────────────────┤
│  DELETE / UPDATE /  │  (via RowLevelOp)     │  RowDelta               │
│  MERGE (MoR)        │                       │  (data + delete files)  │
├─────────────────────┼───────────────────────┼─────────────────────────┤
│  DELETE / UPDATE /  │  (via RowLevelOp)     │  OverwriteFiles         │
│  MERGE (CoW)        │                       │  (full file rewrite)    │
└─────────────────────┴───────────────────────┴─────────────────────────┘
```

---

## 4. Key Classes Reference

| Step          | Class                     | Module       | Key Method                               |
|---------------|---------------------------|--------------|------------------------------------------|
| Entry         | `SparkTable`              | spark/source | `newWriteBuilder()`                      |
| Config        | `SparkWriteBuilder`       | spark/source | `build()`                                |
| Write         | `SparkWrite`              | spark/source | `toBatch()`                              |
| Batch         | `BatchAppend`             | spark/source | `createBatchWriterFactory()`, `commit()` |
| Factory       | `WriterFactory`           | spark/source | `createWriter()`                         |
| Task writer   | `UnpartitionedDataWriter` | spark/source | `write()`, `commit()`                    |
| Task writer   | `PartitionedDataWriter`   | spark/source | `write()`, `commit()`                    |
| Rolling       | `RollingDataWriter`       | core/io      | file size rolling                        |
| Fanout        | `FanoutDataWriter`        | core/io      | multi-partition write                    |
| File factory  | `SparkFileWriterFactory`  | spark/source | `newDataWriter()`                        |
| Base factory  | `BaseFileWriterFactory`   | data         | format-agnostic factory                  |
| Parquet build | `Parquet`                 | parquet      | `writeData()`                            |
| Parquet write | `ParquetWriter`           | parquet      | `add()`, `close()`                       |
| Value encode  | `SparkParquetWriters`     | spark/data   | `buildWriter()`                          |
| Append        | `FastAppend`              | core         | `appendFile()`, `commit()`               |
| Snapshot      | `SnapshotProducer`        | core         | `commit()`, `apply()`                    |
| Manifest      | `ManifestWriter`          | core         | write manifest Avro                      |
| Metadata      | `TableOperations`         | api/core     | `commit(base, updated)`                  |

---

## 5. File Layout After Write

```
table-location/
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json               ◄── new version after commit
│   ├── snap-123456-0-uuid.avro        ◄── manifest list
│   ├── uuid-m0.avro                   ◄── manifest (new data files)
│   └── uuid-m1.avro                   ◄── manifest (existing files, carried over)
│
└── data/
    ├── partition=A/
    │   ├── 00000-0-uuid.parquet       ◄── new data file
    │   └── 00001-0-uuid.parquet       ◄── new data file
    └── partition=B/
        └── 00000-0-uuid.parquet       ◄── new data file
```
