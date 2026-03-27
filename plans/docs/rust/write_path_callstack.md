# Iceberg Rust Write Path — Call Stack

This document traces the complete write path from Arrow RecordBatch data to Parquet file writing and metadata commit in the Rust implementation.

**Related docs:** [architecture_overview.md](architecture_overview.md) | [read_path_callstack.md](read_path_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md)

---

## 1. High-Level Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│  DataFusion SQL:  INSERT INTO catalog.ns.table VALUES (...)          │
│  Or direct API:   IcebergWriter.write(RecordBatch)                   │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                    ┌───────────V───────────┐
                    │  WRITE CONFIGURATION  │
                    │                       │
                    │  IcebergWriterBuilder │
                    │    .build(partition)  │
                    │       │               │
                    │       V               │
                    │  DataFileWriter       │
                    │  (wraps Rolling-      │
                    │   FileWriter wraps    │
                    │   ParquetWriter)      │
                    └───────────┬───────────┘
                                │
                    ┌───────────V───────────┐
                    │  PARTITIONING         │
                    │                       │
                    │  TaskWriter selects:  │
                    │  ┌─────────────────┐  │
                    │  │Unpartitioned    │  │   (no partitioning)
                    │  │Writer           │  │
                    │  ├─────────────────┤  │
                    │  │FanoutWriter     │  │   (unsorted, multi-partition)
                    │  ├─────────────────┤  │
                    │  │ClusteredWriter  │  │   (sorted by partition)
                    │  └─────────────────┘  │
                    └───────────┬───────────┘
                                │
                    ┌───────────V───────────┐
                    │  FILE WRITING         │
                    │                       │
                    │  ParquetWriter        │
                    │    .write(batch)      │
                    │    │                  │
                    │    V                  │
                    │  .parquet files       │
                    │  on object storage    │
                    │                       │
                    │  RollingFileWriter    │
                    │  rolls to new file    │
                    │  when target size hit │
                    └───────────┬───────────┘
                                │ close() → Vec<DataFile>
                                │
                    ┌───────────V───────────┐
                    │  TRANSACTION COMMIT   │
                    │                       │
                    │  Transaction          │
                    │    .fast_append()     │
                    │    .add_data_files()  │
                    │    .apply(tx)         │
                    │       │               │
                    │       V               │
                    │  tx.commit(catalog)   │
                    │       │               │
                    │       V               │
                    │  SnapshotProducer     │
                    │    write manifests    │
                    │    write manifest list│
                    │       │               │
                    │       V               │
                    │  Catalog.update_table │
                    │    atomic commit      │
                    │       │               │
                    │       V               │
                    │  NEW SNAPSHOT         │
                    └───────────────────────┘
```

---

## 2. Detailed Call Stack

### Phase 1: Writer Traits

```
Traits defined in crates/iceberg/src/writer/mod.rs:

IcebergWriterBuilder<I, O>                          [writer/mod.rs]
  │  type R: IcebergWriter<I, O>
  │
  └─> async fn build(
        &self,
        partition_key: Option<PartitionKey>
      ) → Result<Self::R>

IcebergWriter<I = RecordBatch, O = Vec<DataFile>>   [writer/mod.rs]
  │
  ├─> async fn write(&mut self, input: I) → Result<()>
  │     Write one RecordBatch to the current file
  │
  └─> async fn close(&mut self) → Result<O>
        Close writer, return list of written DataFiles

CurrentFileStatus                                    [writer/mod.rs]
  ├─> fn current_file_path() → String
  ├─> fn current_row_num() → usize
  └─> fn current_written_size() → usize
```

### Phase 2: File-Level Writing (Parquet)

```
ParquetWriterBuilder                                [writer/file_writer/parquet_writer.rs]
  │
  ├─> new(WriterProperties, arrow SchemaRef)
  │
  ├─> build_v1(file_io, location, file_name) → ParquetWriter
  ├─> build_v2_data(...) → ParquetWriter
  └─> build_v3_data(...) → ParquetWriter
        │
        └─> ParquetWriter {
              AsyncArrowWriter (parquet crate),
              out: OutputFileWrite,
              current_row_num: usize,
              written_size: usize,
            }

ParquetWriter                                       [writer/file_writer/parquet_writer.rs]
  │
  ├─> write(RecordBatch):
  │     ├─> async_arrow_writer.write(&batch)
  │     ├─> current_row_num += batch.num_rows()
  │     └─> written_size = async_arrow_writer.in_progress_size()
  │
  └─> close() → DataFileBuilder:
        ├─> async_arrow_writer.close()
        │     └─> flushes remaining rows
        │     └─> writes Parquet footer
        ├─> Collect column statistics:
        │     min/max values (from Parquet metadata)
        │     null value counts
        │     NaN value counts (NanValueCountVisitor)
        │     column sizes
        ├─> Compute file size from output stream
        └─> Return DataFileBuilder with:
              file_path, file_format=Parquet,
              record_count, file_size_in_bytes,
              column_sizes, value_counts,
              null_value_counts, nan_value_counts,
              lower_bounds, upper_bounds
```

### Phase 3: Rolling File Writer

```
RollingFileWriterBuilder<B, L, F>                   [writer/file_writer/rolling_writer.rs]
  │  B: FileWriterBuilder (e.g. ParquetWriterBuilder)
  │  L: LocationGenerator
  │  F: FileNameGenerator
  │
  └─> build(partition_key) → RollingFileWriter

RollingFileWriter<B, L, F>                          [writer/file_writer/rolling_writer.rs]
  │
  ├─> write(RecordBatch):
  │     ├─> if current_writer.is_none()
  │     │     OR current_writer.written_size >= target_file_size:
  │     │       close current writer → collect DataFileBuilder
  │     │       open new writer:
  │     │         location = L.generate_location(partition)
  │     │         file_name = F.generate_file_name()
  │     │         writer = B.build(file_io, location, file_name)
  │     │
  │     └─> current_writer.write(batch)
  │
  └─> close() → Vec<DataFileBuilder>:
        close current writer, return all accumulated DataFileBuilders

Target file size: from table properties
  (write.target-file-size-bytes, default 512 MB)
```

### Phase 4: Location & File Name Generation

```
DefaultLocationGenerator                            [writer/file_writer/location_generator.rs]
  │
  └─> generate_location(partition_key):
        table_location / data / partition_path
        e.g. "s3://bucket/table/data/date=2024-01-01/"

DefaultFileNameGenerator                            [writer/file_writer/location_generator.rs]
  │
  └─> generate_file_name():
        {prefix}-{uuid}-{sequence}.{format}
        e.g. "00000-0-a1b2c3d4-e5f6-7890-abcd-ef1234567890.parquet"
```

### Phase 5: Base Writers (Data Files & Delete Files)

```
DataFileWriter<B, L, F>                             [writer/base_writer/data_file_writer.rs]
  │  Wraps RollingFileWriter
  │
  ├─> IcebergWriter.write(RecordBatch):
  │     └─> rolling_writer.write(batch)
  │
  └─> IcebergWriter.close() → Vec<DataFile>:
        ├─> rolling_writer.close() → Vec<DataFileBuilder>
        └─> for each builder:
              builder
                .content(DataContentType::Data)
                .partition(partition_key)
                .build() → DataFile

EqualityDeleteFileWriter<B, L, F>                   [writer/base_writer/equality_delete_writer.rs]
  │  Wraps RollingFileWriter + RecordBatchProjector
  │
  ├─> EqualityDeleteWriterConfig.new(equality_ids, schema):
  │     Validates:
  │       - equality fields must be primitive types
  │       - no floating point types allowed
  │       - fields must not be nullable
  │     Creates RecordBatchProjector for equality columns
  │
  ├─> IcebergWriter.write(RecordBatch):
  │     ├─> projector.project(batch) → projected batch
  │     └─> rolling_writer.write(projected_batch)
  │
  └─> IcebergWriter.close() → Vec<DataFile>:
        ├─> rolling_writer.close() → Vec<DataFileBuilder>
        └─> for each builder:
              builder
                .content(DataContentType::EqualityDeletes)
                .equality_ids(equality_ids)
                .partition(partition_key)
                .build() → DataFile
```

### Phase 6: Partitioning Writers

```
┌─────────────────────────────────────────────────────────────────┐
│ Partitioning Strategy Selection                                 │
│                                                                 │
│  ┌───────────────────────┐                                      │
│  │ UnpartitionedWriter   │  For tables without partition spec   │
│  │                       │  Single inner writer instance        │
│  │ write(batch):         │  Pass-through to inner writer        │
│  │   inner.write(batch)  │                                      │
│  └───────────────────────┘                                      │
│                                                                 │
│  ┌───────────────────────┐                                      │
│  │ FanoutWriter          │  For unsorted/interleaved data       │
│  │                       │  One writer per partition (HashMap)  │
│  │ write(key, batch):    │                                      │
│  │   writers[key]        │  Routes batch to partition writer    │
│  │     .write(batch)     │  Creates writer on first access      │
│  └───────────────────────┘                                      │
│                                                                 │
│  ┌───────────────────────┐                                      │
│  │ ClusteredWriter       │  For pre-sorted data                 │
│  │                       │  Single active writer at a time      │
│  │ write(key, batch):    │                                      │
│  │   if key != current:  │  Closes old writer on partition      │
│  │     close old writer  │  change → more memory efficient      │
│  │     open new writer   │                                      │
│  │   writer.write(batch) │                                      │
│  └───────────────────────┘                                      │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 7: DataFusion TaskWriter

```
TaskWriter<B>                                       [datafusion/src/task_writer.rs]
  │
  ├─> new(schema, partition_spec, table_location,
  │       target_file_size, writer_builder, fanout_enabled):
  │     │
  │     ├─> if unpartitioned:
  │     │     → UnpartitionedWriter<B>
  │     │
  │     ├─> if partitioned && fanout_enabled:
  │     │     → FanoutWriter<B>
  │     │
  │     └─> if partitioned && !fanout_enabled:
  │           → ClusteredWriter<B>
  │
  ├─> write(RecordBatch):
  │     ├─> RecordBatchPartitionSplitter.split(batch)
  │     │     → Vec<(PartitionKey, RecordBatch)>
  │     │     Splits batch into sub-batches by partition key
  │     │     using partition transforms (identity, bucket, truncate, etc.)
  │     │
  │     └─> for each (partition_key, sub_batch):
  │           partitioning_writer.write(partition_key, sub_batch)
  │
  └─> close() → Vec<DataFile>
```

### Phase 8: Transaction & Commit

```
Transaction::new(table)                             [iceberg/src/transaction/mod.rs]
  │
  ├─> Transaction { table, actions: vec![] }
  │
  ├─> fast_append() → FastAppendAction             [transaction/append.rs]
  │     │
  │     ├─> add_data_files(Vec<DataFile>)
  │     ├─> with_check_duplicate(bool)
  │     ├─> set_commit_uuid(Uuid)
  │     └─> set_snapshot_properties(HashMap)
  │
  ├─> Other available actions:
  │     ├─> update_table_properties() → UpdatePropertiesAction
  │     ├─> replace_sort_order() → ReplaceSortOrderAction
  │     ├─> update_location() → UpdateLocationAction
  │     ├─> update_statistics() → UpdateStatisticsAction
  │     └─> upgrade_table_version() → UpgradeFormatVersionAction
  │
  └─> commit(catalog) → Result<Table>              [transaction/mod.rs]
        │
        ├─> Retry loop with exponential backoff:
        │     base: 100ms, max: 5s, max retries: 5
        │
        └─> do_commit():
              │
              ├─> Refresh table from catalog:
              │     catalog.load_table(table_ident)
              │
              ├─> For each action:
              │     action.commit(&table) → ActionCommit {
              │       updates: Vec<TableUpdate>,
              │       requirements: Vec<TableRequirement>,
              │     }
              │
              ├─> Build TableCommit:
              │     TableCommit {
              │       ident: TableIdent,
              │       requirements: all TableRequirements,
              │       updates: all TableUpdates,
              │     }
              │
              └─> catalog.update_table(table_commit)
                    → Returns updated Table
```

### Phase 9: Snapshot Production (FastAppend)

```
FastAppendAction.commit(table)                      [transaction/append.rs]
  │
  ├─> Validate data files (schema, partition spec)
  ├─> Optionally check for duplicate files
  │
  └─> SnapshotProducer::produce_snapshot()          [transaction/snapshot.rs]
        │
        ├─> Generate snapshot_id (unique)
        │
        ├─> Write new manifest file:
        │     ManifestWriter::new(output_file)      [spec/manifest/writer.rs]
        │       │
        │       ├─> For each added DataFile:
        │       │     add_entry(ManifestEntry {
        │       │       status: Added,
        │       │       snapshot_id,
        │       │       data_file,
        │       │     })
        │       │
        │       ├─> Track statistics:
        │       │     added_files_count++
        │       │     added_rows_count += record_count
        │       │     partition summaries (min/max)
        │       │
        │       └─> write_manifest_file()
        │             └─> Serialize entries to Avro format
        │             └─> Write to output file
        │             └─> Return ManifestFile metadata
        │
        ├─> Collect all manifests:
        │     ├─ New manifest (with added files)
        │     └─ Existing manifests (carried forward from parent snapshot)
        │
        ├─> Write manifest list:
        │     ManifestListWriter::new(output_file)  [spec/manifest_list.rs]
        │       └─> Serialize all ManifestFile entries to Avro
        │       └─> Return manifest list location
        │
        └─> Create new Snapshot:
              Snapshot {
                snapshot_id,
                parent_snapshot_id,
                sequence_number (incremented),
                timestamp_ms,
                operation: "append",
                summary: {
                  "added-data-files": N,
                  "added-records": M,
                  "total-data-files": X,
                  "total-records": Y,
                },
                manifest_list: manifest_list_location,
                schema_id,
              }
```

---

## 3. Write Variants

```
┌─────────────────────┬─────────────────────────┬──────────────────────────┐
│  Operation          │  Transaction Action     │  Status in Rust          │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  INSERT / Append    │  FastAppendAction       │  IMPLEMENTED             │
│                     │  (creates new manifest  │                          │
│                     │   with ADDED entries)   │                          │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  INSERT OVERWRITE   │  (not available)        │  NOT IMPLEMENTED         │
│  (dynamic/static)   │                         │  No OverwriteFiles or    │
│                     │                         │  ReplacePartitions       │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  DELETE / UPDATE /  │  (not available)        │  NOT IMPLEMENTED         │
│  MERGE (MoR)        │                         │  No RowDelta action      │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  DELETE / UPDATE /  │  (not available)        │  NOT IMPLEMENTED         │
│  MERGE (CoW)        │                         │  No OverwriteFiles       │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Compaction         │  (not available)        │  NOT IMPLEMENTED         │
│  (RewriteDataFiles) │                         │  No RewriteFiles         │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Properties update  │  UpdatePropertiesAction │  IMPLEMENTED             │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Sort order update  │  ReplaceSortOrderAction │  IMPLEMENTED             │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Format upgrade     │  UpgradeFormatVersion-  │  IMPLEMENTED             │
│                     │  Action                 │                          │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Statistics update  │  UpdateStatisticsAction │  IMPLEMENTED             │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Location update    │  UpdateLocationAction   │  IMPLEMENTED             │
└─────────────────────┴─────────────────────────┴──────────────────────────┘
```

---

## 4. Key Structs Reference

| Step              | Struct/Trait               | File                                                     | Key Method                             |
|-------------------|----------------------------|----------------------------------------------------------|----------------------------------------|
| Writer trait      | `IcebergWriterBuilder`     | iceberg/src/writer/mod.rs                                | `build(partition_key)`                 |
| Writer trait      | `IcebergWriter`            | iceberg/src/writer/mod.rs                                | `write(batch)`, `close()`              |
| Parquet builder   | `ParquetWriterBuilder`     | iceberg/src/writer/file_writer/parquet_writer.rs         | `build_v1()`, `build_v2_data()`        |
| Parquet writer    | `ParquetWriter`            | iceberg/src/writer/file_writer/parquet_writer.rs         | `write()`, `close()`                   |
| Rolling writer    | `RollingFileWriter`        | iceberg/src/writer/file_writer/rolling_writer.rs         | `write()` with auto-rollover           |
| Location gen      | `DefaultLocationGenerator` | iceberg/src/writer/file_writer/location_generator.rs     | `generate_location(partition)`         |
| File name gen     | `DefaultFileNameGenerator` | iceberg/src/writer/file_writer/location_generator.rs     | `generate_file_name()`                 |
| Data writer       | `DataFileWriter`           | iceberg/src/writer/base_writer/data_file_writer.rs       | `write()`, `close() → Vec<DataFile>`   |
| Eq delete writer  | `EqualityDeleteFileWriter` | iceberg/src/writer/base_writer/equality_delete_writer.rs | `write()`, `close()`                   |
| Fanout            | `FanoutWriter`             | iceberg/src/writer/partitioning/fanout_writer.rs         | multi-partition routing                |
| Clustered         | `ClusteredWriter`          | iceberg/src/writer/partitioning/clustered_writer.rs      | sorted partition switching             |
| Unpartitioned     | `UnpartitionedWriter`      | iceberg/src/writer/partitioning/unpartitioned_writer.rs  | pass-through                           |
| Task writer       | `TaskWriter`               | integrations/datafusion/src/task_writer.rs               | `write()`, `close()`                   |
| Transaction       | `Transaction`              | iceberg/src/transaction/mod.rs                           | `fast_append()`, `commit(catalog)`     |
| Append action     | `FastAppendAction`         | iceberg/src/transaction/append.rs                        | `add_data_files()`, `commit()`         |
| Snapshot          | `SnapshotProducer`         | iceberg/src/transaction/snapshot.rs                      | `produce_snapshot()`                   |
| Manifest writer   | `ManifestWriter`           | iceberg/src/spec/manifest/writer.rs                      | `add_entry()`, `write_manifest_file()` |
| Manifest list     | `ManifestListWriter`       | iceberg/src/spec/manifest_list.rs                        | writes manifest list Avro              |
| DataFusion write  | `IcebergTableWrite`        | integrations/datafusion/src/physical_plan/write.rs       | `execute()`                            |
| DataFusion commit | `IcebergTableCommit`       | integrations/datafusion/src/physical_plan/commit.rs      | `execute()`                            |

---

## 5. File Layout After Write

```
table-location/
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json               <── new version after commit
│   ├── snap-123456-0-uuid.avro        <── manifest list
│   ├── uuid-m0.avro                   <── manifest (new data files, status=ADDED)
│   └── uuid-m1.avro                   <── manifest (existing files, carried over)
│
└── data/
    ├── date=2024-01-01/
    │   ├── 00000-0-uuid.parquet       <── new data file
    │   └── 00001-0-uuid.parquet       <── new data file (rolled)
    └── date=2024-01-02/
        └── 00000-0-uuid.parquet       <── new data file
```

---

## 6. Writer Composition Diagram

```
User code / DataFusion
       │
       │  RecordBatch
       V
┌──────────────────────┐
│  TaskWriter          │    (DataFusion integration layer)
│  ┌────────────────┐  │
│  │ Partition-     │  │    Splits by partition key
│  │ Splitter       │  │
│  └───────┬────────┘  │
│          V           │
│  ┌────────────────┐  │
│  │ Partitioning   │  │    Fanout / Clustered / Unpartitioned
│  │ Writer         │  │
│  └───────┬────────┘  │
└──────────┼───────────┘
           V
┌──────────────────────┐
│  DataFileWriter      │    (base writer: sets content type, partition)
│  ┌────────────────┐  │
│  │ Rolling-       │  │    Rolls to new file at target size
│  │ FileWriter     │  │
│  │ ┌────────────┐ │  │
│  │ │ Parquet-   │ │  │    Physical Parquet writing
│  │ │ Writer     │ │  │
│  │ └────────────┘ │  │
│  └────────────────┘  │
└──────────────────────┘
           │
           V
     .parquet files
     on object storage
```
