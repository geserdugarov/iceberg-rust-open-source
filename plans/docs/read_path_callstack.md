# Iceberg Rust Read Path — Call Stack

This document traces the complete read path from a DataFusion SQL query to Parquet file reading in the Rust implementation.

**Related docs:** [architecture_overview.md](architecture_overview.md) | [write_path_callstack.md](write_path_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md) | [update_and_merge_mechanisms.md](update_and_merge_mechanisms.md)

---

## 1. High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│  DataFusion SQL:  SELECT * FROM catalog.ns.table WHERE x>10 │
└───────────────────────────────┬─────────────────────────────┘
                                │
                    ┌───────────V───────────┐
                    │  TABLE RESOLUTION     │
                    │                       │
                    │  IcebergCatalog-      │
                    │  Provider             │
                    │    .schema()          │
                    │       │               │
                    │       V               │
                    │  IcebergSchema-       │
                    │  Provider             │
                    │    .table()           │
                    │       │               │
                    │       V               │
                    │  IcebergTable-        │
                    │  Provider             │
                    │    .scan()            │
                    └───────────┬───────────┘
                                │
                    ┌───────────V───────────┐
                    │  SCAN PLANNING        │
                    │                       │
                    │  IcebergTableScan     │
                    │    .execute()         │
                    │    │                  │
                    │    V                  │
                    │  Table.scan()         │
                    │    │                  │
                    │    V                  │
                    │  TableScanBuilder     │<── filter, column selection,
                    │    .build()           │    snapshot_id
                    │    │                  │
                    │    V                  │
                    │  TableScan            │
                    │    .to_arrow()        │
                    │      .plan_files()    │
                    │    │                  │
                    │    V                  │
                    │  FileScanTask-        │
                    │  Stream               │
                    └───────────┬───────────┘
                                │
                    ┌───────────V───────────┐
                    │  DATA READING         │
                    │                       │
                    │  ArrowReaderBuilder   │
                    │    .build()           │
                    │    │                  │
                    │    V                  │
                    │  ArrowReader          │
                    │    .read(tasks)       │
                    │    │                  │
                    │    V                  │
                    │  FileScanTaskReader   │
                    │    .process() per file│
                    │    │                  │
                    │    ├── Load deletes   │
                    │    ├── Open Parquet   │
                    │    ├── Resolve schema │
                    │    ├── Filter rows    │
                    │    └── Project cols   │
                    │    │                  │
                    │    V                  │
                    │  ArrowRecordBatch-    │
                    │  Stream               │
                    └───────────────────────┘
```

---

## 2. Detailed Call Stack

### Phase 1: Table Resolution (DataFusion Integration)

Providers are cached in `IcebergSchemaProvider` (and built up-front by
`IcebergCatalogProvider::try_new`). At query time, lookup is a cache hit;
fresh metadata is only fetched inside `IcebergTableProvider::scan`.

```
Provider construction (once, eagerly):
  IcebergCatalogProvider::try_new(catalog)            [crates/integrations/datafusion/src/catalog.rs:49]
    └─> for each namespace:
          IcebergSchemaProvider::try_new(catalog, ns) [crates/integrations/datafusion/src/schema.rs]
            └─> for each table:
                  IcebergTableProvider::try_new(...)  [crates/integrations/datafusion/src/table/mod.rs:83]
                    └─> catalog.load_table(TableIdent)  [crates/iceberg/src/catalog/mod.rs]
                    └─> caches Arrow schema (table metadata is NOT cached)

Per-query lookup + scan:
DataFusion SQL Engine
  └─> IcebergCatalogProvider.schema(name)             [crates/integrations/datafusion/src/catalog.rs:95]
        └─> IcebergSchemaProvider.table(name)         [crates/integrations/datafusion/src/schema.rs:127]
              └─> returns the cached Arc<IcebergTableProvider>
                    └─> IcebergTableProvider.scan(...)  [crates/integrations/datafusion/src/table/mod.rs:125]
                          └─> catalog.load_table(TableIdent)  ← fresh metadata, every scan
                          └─> IcebergTableScan::new(table, ...)
```

### Phase 2: Scan Building

```
IcebergTableProvider.scan(                          [crates/integrations/datafusion/src/table/mod.rs:125]
  state, projection, filters, limit)
  │
  └─> Creates IcebergTableScan (ExecutionPlan)      [crates/integrations/datafusion/src/physical_plan/scan.rs:60]
        │  Stores: table, snapshot_id, projection (column names),
        │          predicates (converted via convert_filters_to_predicate),
        │          limit, plan_properties
        │
        └─> IcebergTableScan.execute()              [crates/integrations/datafusion/src/physical_plan/scan.rs:144]
              │
              └─> get_batch_stream()                [crates/integrations/datafusion/src/physical_plan/scan.rs:212]
                    │
                    ├─> table.scan()                [crates/iceberg/src/table.rs:242]
                    │     └─> TableScanBuilder::new(table)
                    │
                    ├─> Apply configuration:
                    │     builder.snapshot_id(id)   (if Some)
                    │     builder.select(column_names) or builder.select_all()
                    │     builder.with_filter(predicate)
                    │
                    └─> builder.build()             [crates/iceberg/src/scan/mod.rs:189]
                          └─> TableScan {
                                column_names,
                                plan_context: Option<PlanContext>,
                                batch_size,
                                file_io,
                                concurrency_limit_data_files,
                                concurrency_limit_manifest_entries,
                                concurrency_limit_manifest_files,
                                row_group_filtering_enabled,
                                row_selection_enabled,
                                runtime,
                              }
```

Note: in the current DataFusion integration the wrapping `get_batch_stream`
only forwards `snapshot_id`, column projection, and the predicate. Other
`TableScanBuilder` knobs (`with_batch_size`, `with_concurrency_limit*`,
`with_row_group_filtering_enabled`, `with_row_selection_enabled`) default to
their builder values; they are surfaced on the lower-level
`TableScan` / `ArrowReaderBuilder` APIs and can be used directly from native
Rust callers.

### Phase 3: File Planning (Concurrent Manifest Processing)

```
TableScan.plan_files()                              [crates/iceberg/src/scan/mod.rs:343-461]
  │
  ├─> PlanContext (built in TableScanBuilder::build):  [crates/iceberg/src/scan/context.rs:149-164]
  │     PlanContext {
  │       snapshot: SnapshotRef,
  │       table_metadata: TableMetadataRef,
  │       snapshot_schema: SchemaRef,
  │       case_sensitive: bool,
  │       predicate: Option<Arc<Predicate>>,         original filter
  │       snapshot_bound_predicate:                  filter bound to schema
  │           Option<Arc<BoundPredicate>>,
  │       object_cache: Arc<ObjectCache>,            manifest/metadata cache
  │       field_ids: Arc<Vec<i32>>,                  projected columns
  │       partition_filter_cache,                    partition filter cache
  │       manifest_evaluator_cache,                  manifest-summary cache
  │       expression_evaluator_cache,                per-entry partition filter
  │     }
  │
  ├─> Create DeleteFileIndex:                       [crates/iceberg/src/delete_file_index.rs]
  │     (delete_file_idx, delete_file_tx) =
  │         DeleteFileIndex::new(runtime)
  │     └─> spawns async task on runtime.io() to collect delete files
  │         from delete_file_tx and build PopulatedDeleteFileIndex:
  │           global_equality_deletes: Vec<Arc<DeleteFileContext>>
  │           eq_deletes_by_partition:
  │               HashMap<Struct, Vec<Arc<DeleteFileContext>>>
  │           pos_deletes_by_partition:
  │               HashMap<Struct, Vec<Arc<DeleteFileContext>>>
  │         State transitions Populating → Populated, gated by a tokio Notify.
  │
  ├─> Fetch and partition-prune manifest list:
  │     plan_context.get_manifest_list()
  │     plan_context.build_manifest_file_contexts(...)
  │       └─> Sort manifest files so Deletes are processed before Data
  │           (avoids data/delete channel deadlock).
  │       └─> For each ManifestFile, when a filter is set:
  │             evaluate ManifestEvaluator against partition summaries;
  │             skip the manifest if it cannot match.
  │
  ├─> Process manifest files concurrently:
  │     For each ManifestFileContext (data or delete):
  │       spawn on runtime.io():
  │         ManifestFileContext.fetch_manifest_and_stream_manifest_entries()
  │           └─> read manifest via ObjectCache
  │           └─> push a ManifestEntryContext per entry to
  │               either the data or delete entry channel.
  │     Concurrency: concurrency_limit_manifest_files.
  │
  ├─> Process DELETE manifest entries concurrently:
  │     For each ManifestEntryContext on the delete channel:
  │       spawn on runtime.cpu():
  │         TableScan::process_delete_manifest_entry()
  │           ├─> Skip if entry is not alive.
  │           ├─> Reject if a data entry appears in a delete manifest.
  │           ├─> ExpressionEvaluator.eval(partition)  → skip non-matching.
  │           └─> Send DeleteFileContext into delete_file_tx,
  │               feeding the DeleteFileIndex above.
  │     Concurrency: concurrency_limit_manifest_entries.
  │
  ├─> Process DATA manifest entries concurrently:
  │     For each ManifestEntryContext on the data channel:
  │       spawn on runtime.cpu():
  │         TableScan::process_data_manifest_entry()
  │           ├─> Skip if entry is not alive.
  │           ├─> Reject if a delete entry appears in a data manifest.
  │           ├─> ExpressionEvaluator.eval(partition)
  │           │     skip if partition can't match.
  │           ├─> InclusiveMetricsEvaluator.eval(data_file)
  │           │     skip if column stats prove no rows can match.
  │           └─> ManifestEntryContext.into_file_scan_task()
  │                                              [scan/context.rs:110-144]
  │                 │
  │                 ├─> delete_file_index.get_deletes_for_data_file()
  │                 │     └─> waits for DeleteFileIndex population if
  │                 │         still in Populating state.
  │                 │     └─> returns applicable Vec<FileScanTaskDeleteFile>.
  │                 │
  │                 └─> yield FileScanTask {
  │                       file_size_in_bytes,
  │                       start: 0, length: file_size_in_bytes,
  │                       record_count: Some(_),
  │                       data_file_path,
  │                       data_file_format,
  │                       schema: SchemaRef,
  │                       project_field_ids: Vec<i32>,
  │                       predicate: Option<BoundPredicate>,
  │                       deletes: Vec<FileScanTaskDeleteFile>,
  │                       partition: Some(Struct),
  │                       partition_spec: None, (TODO)
  │                       name_mapping: None,   (TODO)
  │                       case_sensitive,
  │                     }
  │     Concurrency: concurrency_limit_manifest_entries.
  │
  └─> returns FileScanTaskStream
        (futures MPSC channel-backed BoxStream<Result<FileScanTask>>)
```

### Phase 4: Data Reading (Arrow Reader)

```
ArrowReaderBuilder::new(file_io, runtime)           [crates/iceberg/src/arrow/reader/mod.rs:62]
  │
  ├─> Configuration (defaults / setters):
  │     batch_size                        (Option<usize>, default None)
  │     concurrency_limit_data_files       (default num_cpus)
  │     row_group_filtering_enabled        (default true)
  │     row_selection_enabled              (default false)
  │     parquet_read_options               metadata prefetch,
  │                                        range coalescing, page-index preload
  │
  └─> .build()                                      [crates/iceberg/src/arrow/reader/mod.rs:128]
        └─> ArrowReader {
              batch_size, file_io,
              delete_file_loader: CachingDeleteFileLoader::new(...),
              concurrency_limit_data_files,
              row_group_filtering_enabled,
              row_selection_enabled,
              parquet_read_options,
            }

ArrowReader.read(FileScanTaskStream)                [crates/iceberg/src/arrow/reader/pipeline.rs:48]
  │  (consumes self, returns Result<ScanResult>;
  │   ScanResult wraps the batch stream + ScanMetrics)
  │
  ├─> Build per-scan FileScanTaskReader (cloned per task):
  │     { batch_size, file_io,
  │       delete_file_loader.with_scan_metrics(metrics),
  │       row_group_filtering_enabled, row_selection_enabled,
  │       parquet_read_options, scan_metrics }
  │
  ├─> if concurrency_limit_data_files == 1 (fast path):
  │     task_stream
  │       .and_then(|task| reader.clone().process(task))
  │       .try_flatten()
  │
  └─> else (concurrent path):
        task_stream
          .map_ok(|task| reader.clone().process(task))
          .try_buffer_unordered(concurrency_limit_data_files)
          .try_flatten_unordered(concurrency_limit_data_files)

FileScanTaskReader.process(task)                    [crates/iceberg/src/arrow/reader/pipeline.rs:106]
  │
  ├─> 1. Kick off delete-file loading:
  │     delete_filter_rx = CachingDeleteFileLoader
  │       .load_deletes(&task.deletes, task.schema.clone())
  │       │
  │       ├─> For positional deletes:
  │       │     Read Parquet delete file, build DeleteVector
  │       │     (RoaringTreemap) per data file path,
  │       │     stored in DeleteFilter; finish_pos_del_load notifies waiters.
  │       │
  │       └─> For equality deletes:
  │             Read Parquet delete file once (deduped across data files),
  │             build an unbound Predicate from equality-field values,
  │             store it in DeleteFilter; later tasks await its readiness.
  │
  ├─> 2. Decide whether to preload the Parquet page index:
  │     preload_page_index = (row_selection_enabled && task.predicate.is_some())
  │                         || !task.deletes.is_empty()
  │
  ├─> 3. Open Parquet file once:
  │     ArrowReader::open_parquet_file(path, file_io, file_size,
  │       parquet_read_options, bytes_read_counter)
  │       └─> file_io.new_input(path).reader()
  │       └─> wrapped in CountingFileRead (scan-metrics byte counter)
  │       └─> ArrowFileReader (impl parquet::AsyncFileReader)
  │       └─> ArrowReaderMetadata::load_async()
  │
  ├─> 4. Schema / field-ID resolution (three branches matching Java's
  │      ReadConf, per Iceberg "Column Projection" spec):
  │     ├─> Embedded field IDs (Parquet has PARQUET_FIELD_ID_META_KEY):
  │     │     trust them as-is.
  │     ├─> Name mapping in task: apply_name_mapping_to_arrow_schema().
  │     └─> Fallback: add_fallback_field_ids_to_arrow_schema()
  │           (position-based IDs); projection falls back to position-based
  │           too (missing_field_ids = true).
  │     Then coerce_int96_timestamps for any INT96 columns whose Iceberg
  │     type requires it (avoids arrow-rs i64 overflow).
  │
  ├─> 5. Build ParquetRecordBatchStream:
  │     ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
  │       .with_projection(mask)        ← field-ID or position-based mask
  │       (.with_batch_size(batch_size) if set)
  │
  ├─> 6. Build RecordBatchTransformerBuilder for the output side:
  │     - schema_ref + project_field_ids
  │     - with_constant(RESERVED_FIELD_ID_FILE, file path) if `_file` projected
  │     - with_partition(partition_spec, partition) when both are present
  │
  ├─> 7. Merge filter + equality-delete predicates:
  │     delete_filter = delete_filter_rx.await
  │     delete_predicate = delete_filter.build_equality_delete_predicate(&task)
  │     final_predicate = task.predicate AND delete_predicate (when both)
  │
  ├─> 8. Compute selected row groups / row selection:
  │     a. Byte-range row-group filter from (task.start, task.length)
  │        when either is non-zero (file splitting).
  │     b. If final_predicate is set:
  │          - get_row_filter() → with_row_filter(row_filter)
  │            (per-batch predicate pushdown)
  │          - if row_group_filtering_enabled:
  │              RowGroupMetricsEvaluator → intersect with (a).
  │          - if row_selection_enabled:
  │              PageIndexEvaluator → RowSelection.
  │     c. Positional-delete RowSelection:
  │          delete_filter.get_delete_vector(&task)
  │            → build_deletes_row_selection() (skip deleted rows);
  │          intersect with any predicate-based RowSelection.
  │     Apply with_row_selection() / with_row_groups() on the builder.
  │
  └─> 9. Transform output:
        record_batch_stream.map(|batch|
          record_batch_transformer.process_record_batch(batch))
          │
          ├─> Type promotion / column re-ordering / default columns
          ├─> Inject partition values + virtual `_file` column
          └─> Return ArrowRecordBatchStream
```

---

## 3. Key Structs Reference

| Step              | Struct/Trait                | File                                                   | Key Method                              |
|-------------------|-----------------------------|--------------------------------------------------------|-----------------------------------------|
| Catalog entry     | `IcebergCatalogProvider`    | crates/integrations/datafusion/src/catalog.rs                 | `schema()`                              |
| Schema entry      | `IcebergSchemaProvider`     | crates/integrations/datafusion/src/schema.rs                  | `table()`                               |
| Table provider    | `IcebergTableProvider`      | crates/integrations/datafusion/src/table/mod.rs               | `scan()`                                |
| Execution plan    | `IcebergTableScan`          | crates/integrations/datafusion/src/physical_plan/scan.rs      | `execute()`                             |
| Scan builder      | `TableScanBuilder`          | crates/iceberg/src/scan/mod.rs                                | `build()`, `with_filter()`, `select()`  |
| Scan execution    | `TableScan`                 | crates/iceberg/src/scan/mod.rs                                | `plan_files()`, `to_arrow()`            |
| Plan context      | `PlanContext`               | crates/iceberg/src/scan/context.rs                            | wraps snapshot + caches                 |
| Manifest context  | `ManifestFileContext`       | crates/iceberg/src/scan/context.rs                            | `fetch_manifest_and_stream_manifest_entries()` |
| Entry context     | `ManifestEntryContext`      | crates/iceberg/src/scan/context.rs                            | `into_file_scan_task()`                 |
| Scan task         | `FileScanTask`              | crates/iceberg/src/scan/task.rs                               | data file + deletes + predicate         |
| Delete index      | `DeleteFileIndex`           | crates/iceberg/src/delete_file_index.rs                       | `get_deletes_for_data_file()`           |
| Arrow builder     | `ArrowReaderBuilder`        | crates/iceberg/src/arrow/reader/mod.rs                        | `build()`                               |
| Arrow reader      | `ArrowReader`               | crates/iceberg/src/arrow/reader/mod.rs, crates/iceberg/src/arrow/reader/pipeline.rs | `read()`, `open_parquet_file()`         |
| Per-task reader   | `FileScanTaskReader`        | crates/iceberg/src/arrow/reader/pipeline.rs                   | `process()`                             |
| Parquet adapter   | `ArrowFileReader`           | crates/iceberg/src/arrow/reader/file_reader.rs                | impls `parquet::AsyncFileReader`        |
| Delete filter     | `DeleteFilter`              | crates/iceberg/src/arrow/delete_filter.rs                     | `get_delete_vector()`, `build_equality_delete_predicate()` |
| Delete loader     | `CachingDeleteFileLoader`   | crates/iceberg/src/arrow/caching_delete_file_loader.rs        | `load_deletes()`                        |
| Batch transform   | `RecordBatchTransformer`    | crates/iceberg/src/arrow/record_batch_transformer.rs          | `process_record_batch()`                |
| Partition eval    | `ExpressionEvaluator`       | crates/iceberg/src/expr/visitors/expression_evaluator.rs      | manifest-entry partition pruning        |
| Manifest eval     | `ManifestEvaluator`         | crates/iceberg/src/expr/visitors/manifest_evaluator.rs        | manifest-file partition-summary pruning |
| Metrics eval      | `InclusiveMetricsEvaluator` | crates/iceberg/src/expr/visitors/inclusive_metrics_evaluator.rs | file-level stats pruning              |
| Row group eval    | `RowGroupMetricsEvaluator`  | crates/iceberg/src/expr/visitors/row_group_metrics_evaluator.rs | row-group stats pruning               |
| Page index eval   | `PageIndexEvaluator`        | crates/iceberg/src/expr/visitors/page_index_evaluator.rs      | page-level pruning                      |

---

## 4. Filter Pushdown Pipeline

```
DataFusion Expression (SQL WHERE clause)
       │
       V
IcebergTableScan receives filter predicates
       │
       ├─> convert_filters_to_predicate() converts DataFusion Expr
       │   → Iceberg Predicate            [crates/integrations/datafusion/src/physical_plan/expr_to_predicate.rs]
       │
       V
TableScanBuilder.with_filter(predicate)
       │
       ├─> predicate.rewrite_not()       (Not nodes pushed into children;
       │                                  required by ManifestEvaluator)
       │
       ├─> Two predicate bindings live on `PlanContext`        [crates/iceberg/src/expr/]
       │     - snapshot_bound_predicate: predicate.bind(snapshot_schema, true)
       │       built in TableScanBuilder::build() at scan/mod.rs:278 with
       │       case_sensitive HARD-CODED to `true`. Used by metric/file-level
       │       evaluators (Levels 3-5 below).
       │     - partition_filter (per spec id): predicate.bind(snapshot_schema,
       │       self.case_sensitive) built lazily in
       │       PlanContext::get_partition_filter at scan/context.rs:174.
       │       Used by Levels 1-2 below.
       │   self.case_sensitive also propagates onto every emitted FileScanTask
       │   via ManifestEntryContext::into_file_scan_task (scan/context.rs:142),
       │   and from there is used by
       │   DeleteFilter::build_equality_delete_predicate
       │   (arrow/delete_filter.rs:220) when binding the equality-delete
       │   predicate against the task's schema during data-file reading.
       │
       V
TableScan.plan_files()
       │
       ├─> Level 1: ManifestEvaluator               [crates/iceberg/src/expr/visitors/manifest_evaluator.rs]
       │     Evaluates partition_filter against ManifestFile partition summaries
       │     Skip entire manifests where partition stats can't match
       │
       ├─> Level 2: ExpressionEvaluator             [crates/iceberg/src/expr/visitors/expression_evaluator.rs]
       │     Evaluates predicate against ManifestEntry partition values
       │     Skip files whose partition doesn't match filter
       │
       ├─> Level 3: InclusiveMetricsEvaluator       [crates/iceberg/src/expr/visitors/inclusive_metrics_evaluator.rs]
       │     Evaluates snapshot_bound_predicate against DataFile column statistics
       │     (min/max bounds, null counts, NaN counts)
       │     Skip files where stats prove no rows can match
       │
       ├─> Level 4: RowGroupMetricsEvaluator        [crates/iceberg/src/expr/visitors/row_group_metrics_evaluator.rs]
       │     Parquet row-group metadata (column min/max/nulls)
       │     Skip row groups where stats can't match
       │     (only when row_group_filtering_enabled = true)
       │
       └─> Level 5: PageIndexEvaluator              [crates/iceberg/src/expr/visitors/page_index_evaluator.rs]
             Parquet page-level column index
             Build RowSelection to skip non-matching pages
             (only when row_selection_enabled = true)
```

In addition to predicate-driven pruning, `FileScanTaskReader::process` applies
per-batch row filtering via `ParquetRecordBatchStreamBuilder::with_row_filter`
(built from the merged scan + equality-delete predicate) and a positional-delete
`RowSelection` derived from `DeleteFilter::get_delete_vector`.

---

## 5. Concurrency Model

```
plan_files() Concurrency:
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  ┌──────────────────────────────────────────────────┐        │
│  │ Manifest File Processing (runtime.io().spawn)    │        │
│  │                                                  │        │
│  │  manifest_file_1 ──> [entries] ──> MPSC tx       │        │
│  │  manifest_file_2 ──> [entries] ──> MPSC tx       │        │
│  │  manifest_file_N ──> [entries] ──> MPSC tx       │        │
│  │                                                  │        │
│  │  Concurrency: concurrency_limit_manifest_files   │        │
│  │  Delete manifests are processed before data so   │        │
│  │  the DeleteFileIndex can populate without        │        │
│  │  deadlocking the data channel.                   │        │
│  └─────────────────────────┬────────────────────────┘        │
│                            V                                 │
│  ┌──────────────────────────────────────────────────┐        │
│  │ Entry Processing (runtime.cpu().spawn)           │        │
│  │                                                  │        │
│  │  MPSC rx ──> filter ──> into_file_scan_task()    │        │
│  │  Separate streams for Data vs Delete entries.    │        │
│  │                                                  │        │
│  │  Concurrency: concurrency_limit_manifest_entries │        │
│  └─────────────────────────┬────────────────────────┘        │
│                            V                                 │
│  ┌──────────────────────────────────────────────────┐        │
│  │ Data File Reading (ArrowReader / FileScanTaskRdr)│        │
│  │                                                  │        │
│  │  file_scan_task_1 ──> Parquet read ──> batches   │        │
│  │  file_scan_task_2 ──> Parquet read ──> batches   │        │
│  │  file_scan_task_N ──> Parquet read ──> batches   │        │
│  │                                                  │        │
│  │  Concurrency: concurrency_limit_data_files       │        │
│  │  (single-task fast path when limit == 1)         │        │
│  └──────────────────────────────────────────────────┘        │
│                                                              │
│  Defaults: available_parallelism() (num_cpus) per level.     │
│  All cross-stage communication via futures MPSC channels;    │
│  delete-index synchronization via tokio Notify.              │
│  Async runtime: configurable via Runtime (Tokio-backed),     │
│  with separate io() and cpu() executors.                     │
└──────────────────────────────────────────────────────────────┘
```
