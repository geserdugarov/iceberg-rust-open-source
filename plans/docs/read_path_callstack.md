# Iceberg Rust Read Path — Call Stack

This document traces the complete read path from a DataFusion SQL query to Parquet file reading in the Rust implementation.

**Related docs:** [architecture_overview.md](architecture_overview.md) | [write_path_callstack.md](write_path_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md)

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
                    │  IcebergTable-        │
                    │  Provider             │
                    │    .scan()            │
                    └───────────┬───────────┘
                                │
                    ┌───────────V───────────┐
                    │  SCAN PLANNING        │
                    │                       │
                    │  Table.scan()         │
                    │    │                  │
                    │    V                  │
                    │  TableScanBuilder     │<── filter, column selection,
                    │    .build()           │    snapshot_id, batch_size
                    │    │                  │
                    │    V                  │
                    │  TableScan            │
                    │    .plan_files()      │
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
                    │  process_file_scan_   │
                    │  task() per file      │
                    │    │                  │
                    │    ├── Load deletes   │
                    │    ├── Open Parquet   │
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

```
DataFusion SQL Engine
  └─> IcebergCatalogProvider.schema(name)           [datafusion/src/catalog.rs]
        └─> IcebergSchemaProvider.table(name)
              └─> Catalog.load_table(TableIdent)    [iceberg/src/catalog/mod.rs]
                    └─> returns Table
                          └─> IcebergTableProvider wraps Table
```

### Phase 2: Scan Building

```
IcebergTableProvider.scan(                          [datafusion/src/physical_plan/scan.rs]
  state, projection, filters, limit)
  │
  └─> Creates IcebergTableScan (ExecutionPlan)
        │
        └─> IcebergTableScan.execute()
              │
              └─> get_batch_stream()
                    │
                    ├─> table.scan()                [iceberg/src/table.rs]
                    │     └─> TableScanBuilder::new(table)
                    │
                    ├─> Apply configuration:
                    │     builder.select(column_names)
                    │     builder.with_filter(predicate)
                    │     builder.snapshot_id(id)
                    │     builder.with_batch_size(size)
                    │     builder.with_concurrency_limit_*(n)
                    │     builder.with_row_group_filtering_enabled(bool)
                    │     builder.with_row_selection_enabled(bool)
                    │
                    └─> builder.build()             [iceberg/src/scan/mod.rs]
                          └─> TableScan {
                                column_names,
                                snapshot,
                                file_io,
                                predicate (bound to schema),
                                concurrency limits,
                                row_group_filtering_enabled,
                                row_selection_enabled
                              }
```

### Phase 3: File Planning (Concurrent Manifest Processing)

```
TableScan.plan_files()                              [iceberg/src/scan/mod.rs:338-431]
  │
  ├─> Create PlanContext:                           [iceberg/src/scan/context.rs:150-164]
  │     PlanContext {
  │       snapshot: SnapshotRef,
  │       field_ids: Vec<i32>,            projected columns
  │       snapshot_bound_predicate,       filter bound to schema
  │       object_cache,                   manifest/metadata cache
  │       expression_evaluator_cache,     partition filter cache
  │       metrics_evaluator_cache,        file stats filter cache
  │     }
  │
  ├─> Create DeleteFileIndex:                       [iceberg/src/delete_file_index.rs]
  │     (delete_file_index, delete_file_tx) = DeleteFileIndex::new()
  │     └─> spawns async task to collect delete files
  │         into PopulatedDeleteFileIndex:
  │           global_equality_deletes: Vec<DeleteFileContext>
  │           eq_deletes_by_partition: HashMap<Struct, Vec<DeleteFileContext>>
  │           pos_deletes_by_partition: HashMap<Struct, Vec<DeleteFileContext>>
  │
  ├─> Process DELETE manifest entries concurrently:
  │     for each manifest_file where content == Deletes:
  │       spawn task:
  │         ManifestFileContext.fetch_manifest_and_stream_entries()
  │           └─> read manifest from cache/storage
  │           └─> filter entries by partition predicate
  │           └─> send to delete_file_tx channel
  │
  ├─> Process DATA manifest entries concurrently:
  │     for each manifest_file where content == Data:
  │       spawn task:
  │         ManifestFileContext.fetch_manifest_and_stream_entries()
  │           │
  │           └─> for each ManifestEntry:
  │                 │
  │                 ├─> ExpressionEvaluator.eval(partition)
  │                 │     skip if partition doesn't match filter
  │                 │
  │                 ├─> InclusiveMetricsEvaluator.eval(data_file)
  │                 │     skip if file stats don't match filter
  │                 │
  │                 └─> ManifestEntryContext.into_file_scan_task()
  │                       │                         [scan/context.rs:110-144]
  │                       │
  │                       ├─> delete_file_index.get_deletes_for_data_file()
  │                       │     └─> waits for DeleteFileIndex population
  │                       │     └─> returns applicable delete files
  │                       │
  │                       └─> yield FileScanTask {
  │                             data_file_path,
  │                             data_file_format: Parquet,
  │                             file_size_in_bytes,
  │                             start, length,
  │                             record_count,
  │                             schema,
  │                             project_field_ids,
  │                             predicate: Option<BoundPredicate>,
  │                             deletes: Vec<FileScanTaskDeleteFile>,
  │                             partition,
  │                             partition_spec,
  │                             name_mapping,
  │                             case_sensitive,
  │                           }
  │
  └─> returns FileScanTaskStream
        (MPSC channel-based async stream of FileScanTask)
```

### Phase 4: Data Reading (Arrow Reader)

```
ArrowReaderBuilder.build()                          [iceberg/src/arrow/reader.rs:136-227]
  │
  ├─> Configuration:
  │     batch_size (default 1024)
  │     concurrency_limit_data_files
  │     row_group_filtering_enabled
  │     row_selection_enabled
  │     parquet_read_options (metadata prefetch, coalescing, page index)
  │
  └─> ArrowReader {
        batch_size,
        file_io,
        concurrency_limit_data_files,
        row_group_filtering_enabled,
        row_selection_enabled,
        delete_file_loader: CachingDeleteFileLoader,
        parquet_read_options,
      }

ArrowReader.read(FileScanTaskStream)                [iceberg/src/arrow/reader.rs:247-303]
  │
  ├─> if concurrency == 1 (fast path):
  │     task_stream
  │       .then(|task| process_file_scan_task(task))
  │       .try_flatten()
  │
  └─> if concurrency > 1 (concurrent path):
        task_stream
          .map(|task| process_file_scan_task(task))
          .try_buffer_unordered(concurrency)
          .try_flatten_unordered(concurrency)

ArrowReader.process_file_scan_task(task)            [iceberg/src/arrow/reader.rs:306-400+]
  │
  ├─> 1. Load delete files:
  │     delete_filter = CachingDeleteFileLoader
  │       .load_deletes(&task.deletes, &task.schema)
  │       │
  │       ├─> For position deletes:
  │       │     Read Parquet delete file
  │       │     Build DeleteVector (RoaringTreemap)
  │       │     per data file path
  │       │
  │       └─> For equality deletes:
  │             Read Parquet delete file
  │             Build predicate from equality field values
  │
  ├─> 2. Open Parquet file:
  │     file_io.new_input(data_file_path)
  │       .reader()
  │       └─> ParquetObjectReader or ArrowAsyncFileReader
  │
  ├─> 3. Schema resolution (one of):
  │     ├─> Parquet metadata has Iceberg field IDs → use directly
  │     ├─> Name mapping available → resolve by column names
  │     └─> Fallback: match by column name convention
  │
  ├─> 4. Build ParquetRecordBatchStream:
  │     ParquetRecordBatchStreamBuilder::new(reader)
  │       .with_batch_size(batch_size)
  │       .with_projection(mask)
  │       .with_row_group_filter(predicate)     ← row group pruning
  │       .with_row_selection(selection)         ← page index pruning
  │       .build()
  │
  ├─> 5. Apply delete filter:
  │     For each RecordBatch:
  │       ├─> Build row selection from DeleteVector
  │       │     (skip rows at deleted positions)
  │       └─> Apply equality delete predicate
  │             (filter rows matching delete values)
  │
  └─> 6. Transform output:
        RecordBatchTransformer
          ├─> Project to requested columns
          ├─> Inject partition values as constant columns
          └─> Return ArrowRecordBatchStream
```

---

## 3. Key Structs Reference

| Step              | Struct/Trait                | File                                              | Key Method                              |
|-------------------|-----------------------------|---------------------------------------------------|-----------------------------------------|
| Catalog entry     | `IcebergCatalogProvider`    | integrations/datafusion/src/catalog.rs            | `schema()`                              |
| Table provider    | `IcebergTableProvider`      | integrations/datafusion/src/table/mod.rs          | `scan()`                                |
| Execution plan    | `IcebergTableScan`          | integrations/datafusion/src/physical_plan/scan.rs | `execute()`                             |
| Scan builder      | `TableScanBuilder`          | iceberg/src/scan/mod.rs                           | `build()`, `with_filter()`, `select()`  |
| Scan execution    | `TableScan`                 | iceberg/src/scan/mod.rs                           | `plan_files()`, `to_arrow()`            |
| Plan context      | `PlanContext`               | iceberg/src/scan/context.rs                       | wraps snapshot + caches                 |
| Manifest context  | `ManifestFileContext`       | iceberg/src/scan/context.rs                       | `fetch_manifest_and_stream_entries()`   |
| Entry context     | `ManifestEntryContext`      | iceberg/src/scan/context.rs                       | `into_file_scan_task()`                 |
| Scan task         | `FileScanTask`              | iceberg/src/scan/task.rs                          | data file + deletes + predicate         |
| Delete index      | `DeleteFileIndex`           | iceberg/src/delete_file_index.rs                  | `get_deletes_for_data_file()`           |
| Arrow builder     | `ArrowReaderBuilder`        | iceberg/src/arrow/reader.rs                       | `build()`                               |
| Arrow reader      | `ArrowReader`               | iceberg/src/arrow/reader.rs                       | `read()`, `process_file_scan_task()`    |
| Delete filter     | `DeleteFilter`              | iceberg/src/arrow/delete_filter.rs                | `get_delete_vector()`                   |
| Delete loader     | `CachingDeleteFileLoader`   | iceberg/src/arrow/caching_delete_file_loader.rs   | caches loaded delete files              |
| Batch transform   | `RecordBatchTransformer`    | iceberg/src/arrow/record_batch_transformer.rs     | column projection, partition injection  |
| Partition eval    | `ExpressionEvaluator`       | iceberg/src/expr/visitors/                        | manifest-level partition pruning        |
| Metrics eval      | `InclusiveMetricsEvaluator` | iceberg/src/expr/visitors/                        | file-level stats pruning                |
| Page index eval   | `PageIndexEvaluator`        | iceberg/src/expr/visitors/page_index_evaluator.rs | page-level pruning                      |

---

## 4. Filter Pushdown Pipeline

```
DataFusion Expression (SQL WHERE clause)
       │
       V
IcebergTableScan receives filter predicates
       │
       ├─> Convert DataFusion Expr → Iceberg Predicate
       │
       V
TableScanBuilder.with_filter(predicate)
       │
       ├─> Predicate.bind(schema)                  [iceberg/src/expr/mod.rs]
       │     → BoundPredicate (type-checked against schema)
       │
       V
TableScan.plan_files()
       │
       ├─> Level 1: ManifestEvaluator               [expr/visitors/manifest_evaluator.rs]
       │     Evaluates predicate against ManifestFile partition summaries
       │     Skip entire manifests where partition stats don't match
       │
       ├─> Level 2: ExpressionEvaluator             [expr/visitors/expression_evaluator.rs]
       │     Evaluates predicate against ManifestEntry partition values
       │     Skip files whose partition doesn't match filter
       │
       ├─> Level 3: InclusiveMetricsEvaluator       [expr/visitors/inclusive_metrics_evaluator.rs]
       │     Evaluates predicate against DataFile column statistics
       │     (min/max bounds, null counts, NaN counts)
       │     Skip files where stats prove no rows can match
       │
       ├─> Level 4: Row Group filtering              [arrow/reader.rs]
       │     Parquet row group metadata (column min/max)
       │     Skip row groups where stats don't match
       │     (enabled by row_group_filtering_enabled flag)
       │
       └─> Level 5: Page Index filtering             [expr/visitors/page_index_evaluator.rs]
             Parquet page-level column index
             Build RowSelection to skip non-matching pages
             (enabled by row_selection_enabled flag)
```

---

## 5. Concurrency Model

```
plan_files() Concurrency:
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  ┌──────────────────────────────────────────────────┐        │
│  │ Manifest File Processing (tokio::spawn)          │        │
│  │                                                  │        │
│  │  manifest_file_1 ──> [entries] ──> MPSC tx       │        │
│  │  manifest_file_2 ──> [entries] ──> MPSC tx       │        │
│  │  manifest_file_N ──> [entries] ──> MPSC tx       │        │
│  │                                                  │        │
│  │  Concurrency: concurrency_limit_manifest_files   │        │
│  └─────────────────────────┬────────────────────────┘        │
│                            V                                 │
│  ┌──────────────────────────────────────────────────┐        │
│  │ Entry Processing (tokio::spawn)                  │        │
│  │                                                  │        │
│  │  MPSC rx ──> filter ──> into_file_scan_task()    │        │
│  │                                                  │        │
│  │  Concurrency: concurrency_limit_manifest_entries │        │
│  └─────────────────────────┬────────────────────────┘        │
│                            V                                 │
│  ┌──────────────────────────────────────────────────┐        │
│  │ Data File Reading (ArrowReader)                  │        │
│  │                                                  │        │
│  │  file_scan_task_1 ──> Parquet read ──> batches   │        │
│  │  file_scan_task_2 ──> Parquet read ──> batches   │        │
│  │  file_scan_task_N ──> Parquet read ──> batches   │        │
│  │                                                  │        │
│  │  Concurrency: concurrency_limit_data_files       │        │
│  └──────────────────────────────────────────────────┘        │
│                                                              │
│  Default concurrency: num_cpus for each level                │
│  All communication via futures MPSC channels                 │
│  Async runtime: Tokio                                        │
└──────────────────────────────────────────────────────────────┘
```
