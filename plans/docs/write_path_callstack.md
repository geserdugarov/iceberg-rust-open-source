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
                    │   FileWriter; each    │
                    │   rolled file built   │
                    │   from a FileWriter-  │
                    │   Builder, e.g.       │
                    │   ParquetWriterBuilder│
                    │   → ParquetWriter)    │
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

Two layers of traits cooperate. `IcebergWriter` / `IcebergWriterBuilder` are the
logical, user-facing writers (data file, equality-delete file, partitioning
wrappers). `FileWriter` / `FileWriterBuilder` are the inner physical
file-format writers (Parquet today) used internally by the rolling writer.

```
Logical traits defined in crates/iceberg/src/writer/mod.rs:

IcebergWriterBuilder<I, O>                          [writer/mod.rs]
  │  type R: IcebergWriter<I, O>
  │  (I = RecordBatch, O = Vec<DataFile> by default)
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

Physical traits defined in crates/iceberg/src/writer/file_writer/mod.rs:

FileWriterBuilder<O = Vec<DataFileBuilder>>          [file_writer/mod.rs]
  │  type R: FileWriter<O>
  │
  └─> async fn build(&self, output_file: OutputFile) → Result<Self::R>

FileWriter<O = Vec<DataFileBuilder>>                 [file_writer/mod.rs]
  │  : Send + CurrentFileStatus + 'static
  │
  ├─> async fn write(&mut self, batch: &RecordBatch) → Result<()>
  └─> async fn close(self) → Result<O>   (consumes self)
```

### Phase 2: File-Level Writing (Parquet)

```
ParquetWriterBuilder                                [writer/file_writer/parquet_writer.rs]
  │  Implements FileWriterBuilder<R = ParquetWriter>
  │
  ├─> new(WriterProperties, iceberg SchemaRef)
  │     defaults FieldMatchMode::Id
  │
  ├─> new_with_match_mode(WriterProperties, SchemaRef, FieldMatchMode)
  │
  └─> async fn build(&self, output_file: OutputFile) → ParquetWriter
        │
        └─> ParquetWriter {
              schema: SchemaRef,
              output_file: OutputFile,
              inner_writer: Option<AsyncArrowWriter<…>>,  // lazy
              writer_properties: WriterProperties,
              current_row_num: usize,
              nan_value_count_visitor: NanValueCountVisitor,
            }

ParquetWriter                                       [writer/file_writer/parquet_writer.rs]
  │
  ├─> write(&RecordBatch):
  │     ├─> skip if batch is empty
  │     ├─> current_row_num += batch.num_rows()
  │     ├─> nan_value_count_visitor.compute(schema, batch)
  │     ├─> Lazily initialize AsyncArrowWriter on first non-empty batch
  │     └─> async_arrow_writer.write(&batch)
  │
  ├─> current_written_size():
  │     bytes_written + in_progress_size of the AsyncArrowWriter
  │     (used by RollingFileWriter to decide rollover)
  │
  └─> close(self) → Vec<DataFileBuilder>:
        ├─> async_arrow_writer.finish()
        │     └─> flushes remaining rows
        │     └─> writes Parquet footer
        ├─> If current_row_num == 0: delete the output file, return []
        ├─> Else, collect column statistics from Parquet metadata via
        │     parquet_to_data_file_builder():
        │       lower/upper bounds (MinMaxColAggregator)
        │       column sizes, value counts, null value counts
        │       nan value counts (NanValueCountVisitor)
        │       record_count, file_size_in_bytes, split_offsets
        └─> Return Vec with one DataFileBuilder pre-populated with:
              file_path, file_format=Parquet, record_count,
              file_size_in_bytes, column_sizes, value_counts,
              null_value_counts, nan_value_counts,
              lower_bounds, upper_bounds, split_offsets
              (content type and partition are filled in by the wrapping
               base writer — see Phase 5)
```

### Phase 3: Rolling File Writer

```
RollingFileWriterBuilder<B, L, F>                   [writer/file_writer/rolling_writer.rs]
  │  B: FileWriterBuilder (e.g. ParquetWriterBuilder)
  │  L: LocationGenerator
  │  F: FileNameGenerator
  │
  ├─> new(inner_builder, target_file_size,
  │       file_io, location_generator, file_name_generator)
  │
  ├─> new_with_default_file_size(inner_builder,
  │       file_io, location_generator, file_name_generator)
  │       target = TableProperties::
  │                  PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
  │
  └─> build() → RollingFileWriter
        (no partition argument — partition is supplied on each write)

RollingFileWriter<B, L, F>                          [writer/file_writer/rolling_writer.rs]
  │
  ├─> write(partition_key: &Option<PartitionKey>, &RecordBatch):
  │     ├─> if inner is None:
  │     │     open inner writer via new_output_file(partition_key):
  │     │       loc = L.generate_location(partition_key.as_ref(),
  │     │                                 &F.generate_file_name())
  │     │       inner = B.build(file_io.new_output(loc))
  │     │
  │     ├─> if should_roll() (current_written_size() > target_file_size):
  │     │     data_file_builders.extend(inner.take().close())
  │     │     open a fresh inner writer (same recipe as above)
  │     │
  │     └─> inner.write(batch)
  │
  └─> close(self) → Vec<DataFileBuilder>:
        ├─> if inner is Some: data_file_builders.extend(inner.close())
        └─> return all accumulated DataFileBuilders

Target file size: from table properties
  (write.target-file-size-bytes, default 512 MB)
```

### Phase 4: Location & File Name Generation

```
DefaultLocationGenerator                            [writer/file_writer/location_generator.rs]
  │  Source: table.location() + properties
  │           ("write.data.path" or "write.folder-storage.path";
  │            falling back to "<location>/data")
  │
  └─> generate_location(partition_key: Option<&PartitionKey>, file_name: &str)
        unpartitioned: "{data_location}/{file_name}"
        partitioned:   "{data_location}/{partition_path}/{file_name}"

DefaultFileNameGenerator                            [writer/file_writer/location_generator.rs]
  │  fields: prefix, suffix, format, file_count: AtomicU64
  │
  └─> generate_file_name():
        "{prefix}-{file_count:05}[-{suffix}].{format}"
        e.g. "01900b34-7e0e-7e6b-a26d-2ca345b9d3c1-00000.parquet"
        (DataFusion uses Uuid::now_v7() as the prefix per writer instance —
         see crates/integrations/datafusion/src/physical_plan/write.rs)
```

### Phase 5: Base Writers (Data Files & Delete Files)

```
DataFileWriter<B, L, F>                             [writer/base_writer/data_file_writer.rs]
  │  Wraps RollingFileWriter + remembered Option<PartitionKey>
  │
  ├─> DataFileWriterBuilder::new(RollingFileWriterBuilder)
  ├─> IcebergWriterBuilder.build(partition_key) →
  │     DataFileWriter { inner: rolling_builder.build(), partition_key }
  │
  ├─> IcebergWriter.write(RecordBatch):
  │     └─> rolling.write(&partition_key, &batch)
  │
  └─> IcebergWriter.close() → Vec<DataFile>:
        ├─> rolling.close() → Vec<DataFileBuilder>
        └─> for each builder:
              builder
                .content(DataContentType::Data)
                .partition(pk.data().clone())               // if Some(pk)
                .partition_spec_id(pk.spec().spec_id())     // if Some(pk)
                .build() → DataFile

EqualityDeleteFileWriter<B, L, F>                   [writer/base_writer/equality_delete_writer.rs]
  │  Wraps RollingFileWriter + RecordBatchProjector
  │
  ├─> EqualityDeleteWriterConfig::new(equality_ids, schema):
  │     Builds a RecordBatchProjector for equality columns.
  │     Identifier rules (per Iceberg spec):
  │       - nested types are rejected (mapped to None)
  │       - floating point types are rejected (mapped to None)
  │
  ├─> IcebergWriter.write(RecordBatch):
  │     ├─> projector.project_batch(batch) → projected batch
  │     └─> rolling.write(&partition_key, &projected)
  │
  └─> IcebergWriter.close() → Vec<DataFile>:
        ├─> rolling.close() → Vec<DataFileBuilder>
        └─> for each builder:
              builder
                .content(DataContentType::EqualityDeletes)
                .equality_ids(Some(equality_ids))
                .partition(pk.data().clone())               // if Some(pk)
                .partition_spec_id(pk.spec().spec_id())     // if Some(pk)
                .build() → DataFile
```

There is no position-delete writer yet; only data files and equality-delete files have
base writers in the current codebase.

### Phase 6: Partitioning Writers

```
PartitioningWriter trait                            [writer/partitioning/mod.rs]
  ├─> async fn write(&mut self, PartitionKey, I) → Result<()>
  └─> async fn close(self) → Result<O>

┌─────────────────────────────────────────────────────────────────┐
│ Partitioning Strategy Selection                                 │
│                                                                 │
│  ┌───────────────────────┐                                      │
│  │ UnpartitionedWriter   │  For tables without partition spec   │
│  │                       │  Lazily builds one inner writer      │
│  │ write(batch):         │  (does NOT implement                 │
│  │   inner.write(batch)  │   PartitioningWriter — its           │
│  └───────────────────────┘   write(batch) takes no key)         │
│                                                                 │
│  ┌───────────────────────┐                                      │
│  │ FanoutWriter          │  For unsorted/interleaved data       │
│  │                       │  One writer per partition (HashMap   │
│  │ write(key, batch):    │   keyed by Struct = partition data)  │
│  │   writers[key]        │  Routes batch to partition writer    │
│  │     .write(batch)     │  Creates writer on first access      │
│  └───────────────────────┘                                      │
│                                                                 │
│  ┌───────────────────────┐                                      │
│  │ ClusteredWriter       │  For pre-sorted data                 │
│  │                       │  Single active writer at a time      │
│  │ write(key, batch):    │  On partition change, closes prior   │
│  │   if key != current:  │  writer and records the partition    │
│  │     close old writer  │  as "closed". Writing to a closed    │
│  │     open new writer   │  partition errors → input must be    │
│  │   writer.write(batch) │  sorted by partition key.            │
│  └───────────────────────┘                                      │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 7: DataFusion Write Path

The DataFusion side starts in `IcebergTableProvider::insert_into`
(`integrations/datafusion/src/table/mod.rs`), which assembles the physical plan
that wraps the engine's input stream all the way through to the catalog
commit. `IcebergStaticTableProvider::insert_into` always returns
`FeatureUnsupported` — only the catalog-backed provider supports writes.

```
IcebergTableProvider::insert_into(state, input, _insert_op)
                                                    [datafusion/src/table/mod.rs]
  │
  ├─> Load fresh table metadata from the catalog:
  │     table = catalog.load_table(&self.table_ident).await?
  │
  ├─> Step 1: Project partition values (partitioned tables only)
  │     plan = project_with_partition(input, &table)
  │            [physical_plan/project.rs]
  │     Adds the `_partition` struct column (PROJECTED_PARTITION_VALUE_COLUMN)
  │     computed from each row via PartitionValueCalculator.
  │     Returns input unchanged for unpartitioned tables.
  │
  ├─> Step 2: Repartition for parallel processing
  │     plan = repartition(plan, table_metadata,
  │                        target_partitions = state.config().target_partitions())
  │            [physical_plan/repartition.rs]
  │     - Identity / Bucket transforms → hash partitioning on `_partition`
  │     - Temporal transforms (Year/Month/Day/Hour) → round-robin
  │     - Unpartitioned tables → round-robin
  │
  ├─> Step 3: Optionally sort by partition
  │     fanout_enabled = table_property("write.datafusion.fanout.enabled")
  │                      .unwrap_or(true)
  │     if !fanout_enabled:
  │       plan = sort_by_partition(plan)   [physical_plan/sort.rs]
  │              SortExec on `_partition`, preserve_partitioning = true
  │     (Fanout enabled is the default → no sort node; TaskWriter then picks
  │      FanoutWriter. Disabled → sorted input → TaskWriter picks
  │      ClusteredWriter.)
  │
  ├─> Step 4: Wrap in IcebergWriteExec
  │     plan = IcebergWriteExec::new(table, plan, arrow_schema)
  │            [physical_plan/write.rs]
  │     One write stream per input partition; each stream builds its own
  │     TaskWriter (see below) and emits a one-column RecordBatch of
  │     serialized DataFile JSON (column DATA_FILES_COL_NAME = "data_files").
  │
  ├─> Step 5: Wrap in CoalescePartitionsExec
  │     plan = CoalescePartitionsExec::new(plan)
  │     Merges all per-partition data-file streams into a single stream so
  │     the commit can see every file in one place.
  │
  └─> Step 6: Wrap in IcebergCommitExec and return the final plan
        plan = IcebergCommitExec::new(table, catalog, plan, arrow_schema)
               [physical_plan/commit.rs]
        Executes against partition 0 only:
          - Reads the upstream "data_files" StringArray and
            deserialize_data_file_from_json(...) → Vec<DataFile>
          - tx = Transaction::new(&table)
          - tx.fast_append().add_data_files(data_files).apply(tx)?
                            .commit(catalog).await
          - Emits one UInt64 "count" row with the total record count.

Per-partition execution inside IcebergWriteExec::execute(partition):
  ├─> Read table_properties; require write_format_default == Parquet
  │     (other formats currently return FeatureUnsupported).
  ├─> Build ParquetWriterBuilder with FieldMatchMode::Name and CDC options
  │     derived from table props.
  ├─> Build DefaultLocationGenerator / DefaultFileNameGenerator
  │     (file name prefix = Uuid::now_v7().to_string()).
  ├─> Wrap in RollingFileWriterBuilder(target = write_target_file_size_bytes)
  │     → DataFileWriterBuilder.
  ├─> TaskWriter::try_new(data_file_writer_builder, fanout_enabled,
  │                       schema, partition_spec)
  ├─> For each RecordBatch from the input stream:
  │     task_writer.write(batch).await
  ├─> data_files = task_writer.close().await
  └─> Emit one RecordBatch: StringArray of
        serialize_data_file_to_json(data_file, partition_type, format_version)

TaskWriter<B>                                       [datafusion/src/task_writer.rs]
  │  pub(crate) struct (internal to the datafusion integration)
  │
  ├─> try_new(
  │       writer_builder: B,
  │       fanout_enabled: bool,
  │       schema: SchemaRef,
  │       partition_spec: PartitionSpecRef,
  │     ) → Result<Self>
  │     │
  │     ├─> if partition_spec.is_unpartitioned():
  │     │     → SupportedWriter::Unpartitioned(UnpartitionedWriter::new(b))
  │     │
  │     ├─> if partitioned && fanout_enabled:
  │     │     → SupportedWriter::Fanout(FanoutWriter::new(b))
  │     │
  │     └─> if partitioned && !fanout_enabled:
  │           → SupportedWriter::Clustered(ClusteredWriter::new(b))
  │     +
  │     For partitioned tables, also builds a
  │     RecordBatchPartitionSplitter (iceberg::arrow) using precomputed
  │     partition values from the upstream `_partition` column.
  │
  ├─> async fn write(&mut self, RecordBatch):
  │     ├─> Unpartitioned: writer.write(batch)        (no split)
  │     └─> Fanout / Clustered:
  │           splitter.split(batch) → Vec<(PartitionKey, RecordBatch)>
  │           for each (key, sub_batch):
  │               writer.write(key, sub_batch)
  │
  └─> async fn close(self) → Result<Vec<DataFile>>   (consumes self)
```

### Phase 8: Transaction & Commit

```
Transaction::new(table: &Table)                     [iceberg/src/transaction/mod.rs]
  │
  ├─> Transaction { table: table.clone(), actions: vec![] }
  │
  ├─> fast_append() → FastAppendAction              [transaction/append.rs]
  │     │
  │     ├─> add_data_files(impl IntoIterator<Item = DataFile>)
  │     ├─> with_check_duplicate(bool)              (default true)
  │     ├─> set_commit_uuid(Uuid)
  │     ├─> set_key_metadata(Vec<u8>)
  │     └─> set_snapshot_properties(HashMap<String, String>)
  │
  ├─> Other action constructors on Transaction:
  │     ├─> update_table_properties() → UpdatePropertiesAction
  │     ├─> update_schema()           → UpdateSchemaAction
  │     ├─> replace_sort_order()      → ReplaceSortOrderAction
  │     ├─> update_location()         → UpdateLocationAction
  │     ├─> update_statistics()       → UpdateStatisticsAction
  │     └─> upgrade_table_version()   → UpgradeFormatVersionAction
  │
  ├─> Actions are attached via the ApplyTransactionAction trait:
  │     let tx = action.apply(tx)?;
  │     (pushes Arc<dyn TransactionAction> into tx.actions)
  │
  └─> commit(catalog: &dyn Catalog) → Result<Table>  [transaction/mod.rs]
        │
        ├─> Empty actions → return original table unchanged
        │
        ├─> Build exponential backoff (backon::ExponentialBuilder)
        │   from table properties (with documented defaults):
        │     commit.retry.min-wait-ms     (default 100 ms)
        │     commit.retry.max-wait-ms     (default 60 000 ms = 1 min)
        │     commit.retry.total-timeout-ms(default 1 800 000 ms = 30 min)
        │     commit.retry.num-retries     (default 4)
        │   Retries while err.retryable() is true.
        │
        └─> do_commit():
              │
              ├─> Refresh table from catalog:
              │     catalog.load_table(table_ident)
              │     If metadata or metadata_location changed,
              │     re-base on the refreshed table.
              │
              ├─> For each action:
              │     action.commit(&current_table) → ActionCommit {
              │       updates: Vec<TableUpdate>,
              │       requirements: Vec<TableRequirement>,
              │     }
              │     Apply updates locally to current_table so subsequent
              │     actions see a consistent view.
              │
              ├─> Build TableCommit:
              │     TableCommit::builder()
              │       .ident(table_ident)
              │       .requirements(all TableRequirements)
              │       .updates(all TableUpdates)
              │       .build()
              │
              └─> catalog.update_table(table_commit)
                    → Returns updated Table
```

### Phase 9: Snapshot Production (FastAppend)

```
FastAppendAction.commit(table)                      [transaction/append.rs]
  │
  ├─> Build SnapshotProducer::new(table, commit_uuid, key_metadata,
  │                               snapshot_properties, added_data_files)
  ├─> validate_added_data_files()
  │     - only DataContentType::Data allowed
  │     - partition_spec_id must match table default
  │     - partition values must match partition type
  ├─> if check_duplicate: validate_duplicate_files()
  └─> SnapshotProducer::commit(FastAppendOperation,
                               DefaultManifestProcess)
        → ActionCommit (TableUpdate::AddSnapshot, SetSnapshotRef, …)

SnapshotProducer::commit()                          [transaction/snapshot.rs]
  │
  ├─> Generate manifest list path:
  │     "{table_location}/metadata/snap-{snapshot_id}-{attempt}-{commit_uuid}.avro"
  │
  ├─> Open ManifestListWriter (version-specific constructor):
  │     ├─ FormatVersion::V1 → ManifestListWriter::v1(...)
  │     ├─ FormatVersion::V2 → ManifestListWriter::v2(...)
  │     └─ FormatVersion::V3 → ManifestListWriter::v3(..., first_row_id)
  │     [spec/manifest_list.rs]
  │
  ├─> Write new manifest file with added entries:
  │     write_added_manifest() → ManifestFile
  │       │
  │       ├─> Build per-version writer via ManifestWriterBuilder
  │       │     (build_v1 / build_v2_data / build_v3_data, ...)
  │       │     [spec/manifest/writer.rs]
  │       │
  │       ├─> For each added DataFile:
  │       │     add_entry(ManifestEntry {
  │       │       status: Added,
  │       │       snapshot_id, (V1 only; inherited for V2/V3)
  │       │       data_file,
  │       │     })
  │       │
  │       └─> writer.write_manifest_file()
  │             → Serialize entries to Avro and return ManifestFile metadata
  │
  ├─> Gather existing manifests from FastAppendOperation.existing_manifest():
  │     load parent snapshot's manifest list, carry forward all entries
  │     that have added or existing files.
  │
  ├─> manifest_list_writer.add_manifests(all_manifests).close()
  │
  ├─> Build Summary via SnapshotSummaryCollector:
  │     Operation::Append
  │     additional_properties:
  │       added-data-files, added-records, added-files-size,
  │       total-data-files, total-records, total-files-size, …
  │     merged with snapshot_properties.
  │
  └─> Emit TableUpdates:
        AddSnapshot { snapshot }, SetSnapshotRef { MAIN, snapshot_id }
        Snapshot fields:
          snapshot_id, parent_snapshot_id,
          sequence_number (next_sequence_number from metadata),
          timestamp_ms, manifest_list, summary, schema_id,
          first_row_id (V3 only).
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
│  MERGE (MoR)        │                         │  EqualityDeleteFileWriter│
│                     │                         │  exists but no RowDelta  │
│                     │                         │  action wires its files. │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  DELETE / UPDATE /  │  (not available)        │  NOT IMPLEMENTED         │
│  MERGE (CoW)        │                         │  No OverwriteFiles       │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Compaction         │  (not available)        │  NOT IMPLEMENTED         │
│  (RewriteDataFiles) │                         │  No RewriteFiles         │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Properties update  │  UpdatePropertiesAction │  IMPLEMENTED             │
├─────────────────────┼─────────────────────────┼──────────────────────────┤
│  Schema update      │  UpdateSchemaAction     │  IMPLEMENTED             │
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

| Step              | Struct/Trait               | File                                                     | Key Method                              |
|-------------------|----------------------------|----------------------------------------------------------|-----------------------------------------|
| Logical builder   | `IcebergWriterBuilder`     | iceberg/src/writer/mod.rs                                | `build(partition_key)`                  |
| Logical writer    | `IcebergWriter`            | iceberg/src/writer/mod.rs                                | `write(batch)`, `close()`               |
| Physical builder  | `FileWriterBuilder`        | iceberg/src/writer/file_writer/mod.rs                    | `build(output_file)`                    |
| Physical writer   | `FileWriter`               | iceberg/src/writer/file_writer/mod.rs                    | `write(&batch)`, `close(self)`          |
| Parquet builder   | `ParquetWriterBuilder`     | iceberg/src/writer/file_writer/parquet_writer.rs         | `new`, `new_with_match_mode`, `build`   |
| Parquet writer    | `ParquetWriter`            | iceberg/src/writer/file_writer/parquet_writer.rs         | `write`, `close → Vec<DataFileBuilder>` |
| Rolling builder   | `RollingFileWriterBuilder` | iceberg/src/writer/file_writer/rolling_writer.rs         | `new`, `new_with_default_file_size`, `build` |
| Rolling writer    | `RollingFileWriter`        | iceberg/src/writer/file_writer/rolling_writer.rs         | `write(&pk, &batch)` with auto-rollover |
| Location gen      | `DefaultLocationGenerator` | iceberg/src/writer/file_writer/location_generator.rs     | `generate_location(pk, file_name)`      |
| File name gen     | `DefaultFileNameGenerator` | iceberg/src/writer/file_writer/location_generator.rs     | `generate_file_name()`                  |
| Data writer       | `DataFileWriter`           | iceberg/src/writer/base_writer/data_file_writer.rs       | `write()`, `close() → Vec<DataFile>`    |
| Eq delete writer  | `EqualityDeleteFileWriter` | iceberg/src/writer/base_writer/equality_delete_writer.rs | `write()`, `close()`                    |
| Partition trait   | `PartitioningWriter`       | iceberg/src/writer/partitioning/mod.rs                   | `write(pk, batch)`, `close(self)`       |
| Fanout            | `FanoutWriter`             | iceberg/src/writer/partitioning/fanout_writer.rs         | multi-partition routing                 |
| Clustered         | `ClusteredWriter`          | iceberg/src/writer/partitioning/clustered_writer.rs      | sorted partition switching              |
| Unpartitioned     | `UnpartitionedWriter`      | iceberg/src/writer/partitioning/unpartitioned_writer.rs  | pass-through (no key in `write`)        |
| Task writer       | `TaskWriter`               | integrations/datafusion/src/task_writer.rs               | `try_new()`, `write()`, `close(self)`   |
| Transaction       | `Transaction`              | iceberg/src/transaction/mod.rs                           | `fast_append()`, `commit(catalog)`      |
| Action helper     | `ApplyTransactionAction`   | iceberg/src/transaction/action.rs                        | `apply(tx) → Transaction`               |
| Append action     | `FastAppendAction`         | iceberg/src/transaction/append.rs                        | `add_data_files()`, `commit()`          |
| Snapshot          | `SnapshotProducer`         | iceberg/src/transaction/snapshot.rs                      | `commit(op, manifest_process)`          |
| Manifest builder  | `ManifestWriterBuilder`    | iceberg/src/spec/manifest/writer.rs                      | `build_v1` / `build_v2_data` / `build_v3_data` / `…_deletes` |
| Manifest writer   | `ManifestWriter`           | iceberg/src/spec/manifest/writer.rs                      | `add_entry`, `add_file`, `write_manifest_file` |
| Manifest list     | `ManifestListWriter`       | iceberg/src/spec/manifest_list.rs                        | `v1` / `v2` / `v3`, `add_manifests`, `close` |
| DataFusion write  | `IcebergWriteExec`         | integrations/datafusion/src/physical_plan/write.rs       | `execute()`                             |
| DataFusion commit | `IcebergCommitExec`        | integrations/datafusion/src/physical_plan/commit.rs      | `execute()`                             |

---

## 5. File Layout After Write

```
table-location/
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json                              <── new version after commit
│   ├── snap-<snapshot_id>-0-<commit_uuid>.avro       <── new manifest list
│   │                                                    (references the new manifest below
│   │                                                     plus parent-snapshot manifests by path)
│   ├── <commit_uuid>-m0.avro                         <── new manifest (added data files, status=ADDED)
│   └── <prev_commit_uuid>-m0.avro                    <── pre-existing manifests are NOT rewritten;
│                                                        fast append references them in-place
│                                                        from the new manifest list
│
└── data/
    ├── region=US/
    │   ├── <writer_uuid>-00000.parquet               <── new data file
    │   └── <writer_uuid>-00001.parquet               <── new data file (rolled)
    └── region=EU/
        └── <writer_uuid>-00000.parquet               <── new data file
```

Fast append writes exactly one new manifest per commit (for the added data
files); manifests from prior snapshots are not copied or rewritten — they are
carried forward into the new manifest list by their existing paths (see
`FastAppendOperation::existing_manifest` in `transaction/append.rs`).

Data file names follow `{prefix}-{file_count:05}[-{suffix}].{format}` from
`DefaultFileNameGenerator`. The DataFusion integration uses
`Uuid::now_v7().to_string()` as the prefix for each writer instance, giving
files like `01900b34-7e0e-7e6b-a26d-2ca345b9d3c1-00000.parquet`.

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
│  │ RecordBatch-   │  │    Splits by partition key
│  │ PartitionSplit │  │    (only for partitioned tables)
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
│  │ Rolling-       │  │    Rolls to new file when
│  │ FileWriter     │  │    current_written_size() > target
│  │ ┌────────────┐ │  │
│  │ │ Parquet-   │ │  │    Physical Parquet writing
│  │ │ Writer     │ │  │    (built per file from
│  │ └────────────┘ │  │     ParquetWriterBuilder)
│  └────────────────┘  │
└──────────────────────┘
           │
           V
     .parquet files
     on object storage
```
