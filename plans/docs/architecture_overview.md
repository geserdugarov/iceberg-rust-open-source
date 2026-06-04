# Apache Iceberg Rust — Architecture Overview

This document provides a high-level architecture overview of the Apache Iceberg Rust implementation (`iceberg-rust`).

**Related docs:** [read_path_callstack.md](read_path_callstack.md) | [write_path_callstack.md](write_path_callstack.md) | [compaction_callstack.md](compaction_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md) | [update_and_merge_mechanisms.md](update_and_merge_mechanisms.md)

---

## 1. Layered Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        QUERY ENGINES                                │
│   ┌────────────────┐  ┌─────────────────────────────────────────┐   │
│   │  DataFusion    │  │  Custom Applications (via iceberg crate)│   │
│   │  (SQL + DF API)│  │  using IcebergWriter / TableScan API    │   │
│   └───────┬────────┘  └───────────────────┬─────────────────────┘   │
│           │                               │                         │
└───────────┼───────────────────────────────┼─────────────────────────┘
            │                               │
┌───────────V───────────────────────────────V─────────────────────────┐
│                   ENGINE INTEGRATIONS                               │
│   ┌─────────────────────┐  ┌──────────────────────────────────┐     │
│   │ iceberg-datafusion  │  │ iceberg-cache-moka               │     │
│   │ (IcebergTableScan,  │  │ (metadata caching)               │     │
│   │  IcebergTableWrite, │  │                                  │     │
│   │  IcebergTableCommit,│  │                                  │     │
│   │  TaskWriter)        │  │                                  │     │
│   └─────────┬───────────┘  └──────────────┬───────────────────┘     │
│             │                             │                         │
└─────────────┼─────────────────────────────┼─────────────────────────┘
              │                             │
              └─────────────┬───────────────┘
                            │
┌───────────────────────────V─────────────────────────────────────────┐
│                     ICEBERG CORE  (crates/iceberg)                  │
│                                                                     │
│   Catalog, Table, Schema, PartitionSpec, SortOrder, Snapshot        │
│   TableScan, Transaction, FastAppendAction, SnapshotProducer        │
│   IcebergWriter, DataFileWriter, EqualityDeleteFileWriter           │
│   ManifestReader, ManifestWriter, ManifestListWriter                │
│   DeleteFileIndex, DeleteVector, DeleteFilter                       │
│   Expression, BoundPredicate, Transform                             │
│                                                                     │
└────────────┬──────────────────────┬─────────────────────────────────┘
             │                      │
┌────────────V─────────┐  ┌─────────V─────────────────────────────────┐
│  FILE FORMATS        │  │  CATALOG IMPLEMENTATIONS                  │
│                      │  │                                           │
│  ┌────────────────┐  │  │  ┌──────┐ ┌─────┐ ┌──────┐ ┌────────┐     │
│  │ arrow-rs /     │  │  │  │ REST │ │ HMS │ │ Glue │ │S3Tables│     │
│  │ parquet crate  │  │  │  └──────┘ └─────┘ └──────┘ └────────┘     │
│  │ (Parquet R/W)  │  │  │  ┌─────┐  ┌────────┐ ┌──────────────┐     │
│  ├────────────────┤  │  │  │ SQL │  │ Memory │ │ CatalogLoader│     │
│  │ apache-avro    │  │  │  └─────┘  └────────┘ └──────────────┘     │
│  │ (Manifest R/W) │  │  │                                           │
│  └────────────────┘  │  └───────────────────────────────────────────┘
│                      │
└────────────┬─────────┘
             │
┌────────────V────────────────────────────────────────────────────────┐
│                     STORAGE LAYER                                   │
│                                                                     │
│   Storage trait  +  StorageFactory trait                            │
│   ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐    │
│   │ OpenDAL      │  │ LocalFs      │  │ Memory                 │    │
│   │ (S3, GCS,    │  │ Storage      │  │ Storage                │    │
│   │  ADLS, etc.) │  │              │  │ (testing)              │    │
│   └──────────────┘  └──────────────┘  └────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
             │
┌────────────V────────────────────────────────────────────────────────┐
│                        CLOUD STORAGE                                │
│    S3  |  GCS  |  ADLS  |  HDFS  |  Local FS                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Workspace Structure

```
iceberg-rust/
├── crates/
│   ├── iceberg/                    Core library — types, specs, readers, writers, transforms
│   │   └── src/
│   │       ├── catalog/            Catalog trait + MemoryCatalog
│   │       ├── io/                 FileIO, Storage trait, InputFile, OutputFile
│   │       ├── spec/               Iceberg spec types (Schema, Snapshot, Manifest, DataFile...)
│   │       ├── scan/               TableScan, FileScanTask, PlanContext
│   │       ├── writer/             IcebergWriter, DataFileWriter, Parquet writer, partitioning
│   │       ├── transaction/        Transaction, FastAppendAction, SnapshotProducer
│   │       ├── arrow/              ArrowReader, DeleteFilter, RecordBatchTransformer
│   │       ├── expr/               Expressions, predicates, evaluator visitors
│   │       ├── transform/          Partition transforms (identity, bucket, truncate, etc.)
│   │       ├── puffin/             Puffin file format (for statistics / deletion vectors)
│   │       ├── inspect/            Metadata inspection utilities
│   │       ├── delete_file_index.rs   Index of delete files for scan planning
│   │       ├── delete_vector.rs       RoaringTreemap-based position delete bitmap
│   │       ├── metadata_columns.rs    Metadata column definitions (_file, _pos, etc.)
│   │       └── table.rs               Table abstraction
│   │
│   ├── catalog/
│   │   ├── rest/                   REST catalog (HTTP-based)
│   │   ├── hms/                    Hive Metastore catalog (Thrift)
│   │   ├── glue/                   AWS Glue catalog
│   │   ├── s3tables/               AWS S3 Tables catalog
│   │   ├── sql/                    SQL-based catalog (PostgreSQL, MySQL, SQLite)
│   │   └── loader/                 Unified catalog loader (dynamic dispatch)
│   │
│   ├── storage/
│   │   └── opendal/                OpenDAL storage backend (S3, GCS, ADLS, HDFS, etc.)
│   │
│   ├── integrations/
│   │   ├── datafusion/             DataFusion query engine integration
│   │   │   └── src/
│   │   │       ├── physical_plan/  IcebergTableScan, IcebergTableWrite, IcebergTableCommit
│   │   │       ├── task_writer.rs  High-level writer with partition dispatch
│   │   │       └── ...             Table provider, catalog provider
│   │   ├── cache-moka/             Moka-based metadata caching layer
│   │   └── playground/             Playground for exploration
│   │
│   ├── test_utils/                 Shared test utilities (catalog setup, assertion helpers)
│   ├── integration_tests/          Integration test suite (requires Docker)
│   ├── sqllogictest/               SQL logic tests
│   └── examples/                   Example applications
│
├── bindings/
│   └── python/                     Python bindings (excluded from workspace, separate build)
│
├── Cargo.toml                      Workspace manifest
├── Cargo.lock                      Committed for reproducible builds
├── Makefile                        Build, lint, test commands
└── rust-toolchain.toml             Nightly toolchain (for rustfmt/clippy)
```

---

## 3. Table Metadata Structure

```
                    ┌─────────────────────────┐
                    │   metadata.json (v2/v3) │
                    │                         │
                    │  format-version         │
                    │  table-uuid             │
                    │  location               │
                    │  schemas[]              │
                    │  partition-specs[]      │
                    │  sort-orders[]          │
                    │  current-snapshot-id ───┼──┐
                    │  snapshots[] ───────────┼──┤
                    │  properties{}           │  │
                    └─────────────────────────┘  │
                                                 │
                    ┌────────────────────────────┘
                    V
            ┌───────────────────┐
            │    Snapshot       │
            │                   │
            │  snapshot-id      │
            │  parent-id        │
            │  sequence-number  │
            │  timestamp-ms     │
            │  operation        │
            │  summary{}        │
            │  manifest-list ───┼──┐
            └───────────────────┘  │
                                   │
                    ┌──────────────┘
                    V
          ┌──────────────────────┐
          │  Manifest List       │    (Avro file: snap-<id>-<attempt>.avro)
          │                      │
          │  ┌────────────────┐  │
          │  │ ManifestFile 1 ├──┼──┐
          │  ├────────────────┤  │  │
          │  │ ManifestFile 2 │  │  │   Each entry contains:
          │  ├────────────────┤  │  │   - manifest path
          │  │ ManifestFile N │  │  │   - partition spec id
          │  └────────────────┘  │  │   - added/existing/deleted counts
          └──────────────────────┘  │   - partition field summaries
                                    │     (min/max for partition pruning)
                    ┌───────────────┘
                    V
          ┌──────────────────────┐
          │  Manifest File       │    (Avro file: <uuid>-m<N>.avro)
          │                      │
          │  ┌────────────────┐  │
          │  │ ManifestEntry 1├──┼──┐
          │  ├────────────────┤  │  │
          │  │ ManifestEntry 2│  │  │   Each entry contains:
          │  ├────────────────┤  │  │   - status (ADDED/EXISTING/DELETED)
          │  │ ManifestEntry N│  │  │   - snapshot-id
          │  └────────────────┘  │  │   - data-file reference
          └──────────────────────┘  │
                                    │
                    ┌───────────────┘
                    V
          ┌──────────────────────┐
          │  DataFile            │
          │                      │
          │  file-path           │    Actual data files:
          │  file-format         │    - Parquet (.parquet)
          │  content-type        │    (Data / PositionDeletes /
          │  partition           │     EqualityDeletes)
          │  record-count        │
          │  file-size-bytes     │
          │  column-sizes{}      │    Per-column stats:
          │  value-counts{}      │    - null counts
          │  null-value-counts{} │    - lower/upper bounds
          │  lower-bounds{}      │    - NaN counts
          │  upper-bounds{}      │
          └──────────────────────┘

Rust types (crates/iceberg/src/spec/):
  TableMetadata      → table_metadata.rs
  Snapshot           → snapshot.rs
  ManifestList       → manifest_list.rs
  ManifestFile       → manifest_list.rs (ManifestFile struct)
  ManifestEntry      → manifest/mod.rs
  DataFile           → manifest/data_file.rs
  Schema             → schema.rs
  PartitionSpec      → partition.rs
  SortOrder          → sort.rs
```

---

## 4. Key Traits and Abstractions

```
                        ┌─────────────────────┐
                        │   Catalog trait     │   crates/iceberg/src/catalog/mod.rs
                        │   #[async_trait]    │
                        │                     │
                        │ list_namespaces()   │
                        │ create_namespace()  │
                        │ list_tables()       │
                        │ create_table()      │
                        │ load_table()      ──┼──> returns Table
                        │ drop_table()        │
                        │ rename_table()      │
                        │ update_table()      │   <── atomic commit via TableCommit
                        └────────┬────────────┘
                                 │ returns
                                 V
                        ┌─────────────────────┐
                        │     Table           │   crates/iceberg/src/table.rs
                        │                     │
                        │ metadata()        ──┼──> TableMetadata
                        │ file_io()         ──┼──> FileIO
                        │ scan()            ──┼──┐  READS
                        │                     │  │
                        └─────────────────────┘  │
                                 ┌───────────────┘
                                 V
                       ┌───────────────────┐
                       │ TableScanBuilder  │   crates/iceberg/src/scan/mod.rs
                       │                   │
                       │ select(columns)   │
                       │ with_filter(expr) │
                       │ snapshot_id(id)   │
                       │ build()         ──┼──> TableScan
                       └───────────────────┘
                                 │
                                 V
                       ┌───────────────────┐
                       │ TableScan         │
                       │                   │
                       │ plan_files()    ──┼──> FileScanTaskStream
                       │ to_arrow()      ──┼──> ArrowRecordBatchStream
                       └───────────────────┘


                    ┌───────────────────────┐
                    │  WRITE SIDE           │
                    │                       │
                    │  IcebergWriterBuilder │   crates/iceberg/src/writer/mod.rs
                    │    build()          ──┼──> IcebergWriter
                    │                       │
                    │  IcebergWriter        │
                    │    write(RecordBatch) │
                    │    close()          ──┼──> Vec<DataFile>
                    │                       │
                    └──────────┬────────────┘
                               │
                    ┌──────────V────────────┐
                    │  Transaction          │   crates/iceberg/src/transaction/mod.rs
                    │                       │
                    │  fast_append()      ──┼──> FastAppendAction
                    │  update_table_        │
                    │    properties()       │
                    │  replace_sort_order() │
                    │  commit(catalog)    ──┼──> atomic metadata update
                    └───────────────────────┘


                    ┌───────────────────────┐
                    │  Storage trait        │   crates/iceberg/src/io/storage/mod.rs
                    │  #[typetag::serde]    │
                    │                       │
                    │  exists(path)         │
                    │  read(path)           │
                    │  write(path, bytes)   │
                    │  reader(path)       ──┼──> Box<dyn FileRead>
                    │  writer(path)       ──┼──> Box<dyn FileWrite>
                    │  delete(path)         │
                    │  new_input(path)    ──┼──> InputFile
                    │  new_output(path)   ──┼──> OutputFile
                    └───────────────────────┘

                    ┌───────────────────────┐
                    │  StorageFactory trait │   crates/iceberg/src/io/storage/mod.rs
                    │  #[typetag::serde]    │
                    │                       │
                    │  build(config)      ──┼──> Arc<dyn Storage>
                    └───────────────────────┘

                    ┌───────────────────────┐
                    │  FileIO               │   crates/iceberg/src/io/file_io.rs
                    │                       │
                    │  Wraps Storage with   │
                    │  lazy init via        │
                    │  OnceLock<Storage>    │
                    │                       │
                    │  new_input(path)      │
                    │  new_output(path)     │
                    └───────────────────────┘
```

---

## 5. Catalog Subsystem

```
┌──────────────────────────────────────────────────────────────────┐
│                 DataFusion Catalog Layer                         │
│                                                                  │
│  ┌──────────────────────────┐                                    │
│  │ IcebergCatalogProvider   │   crates/integrations/datafusion/  │
│  │ (implements DataFusion   │   src/catalog.rs                   │
│  │  CatalogProvider)        │                                    │
│  └───────────┬──────────────┘                                    │
│              │ delegates to                                      │
└──────────────┼───────────────────────────────────────────────────┘
               │
┌──────────────V───────────────────────────────────────────────────┐
│                 Iceberg Catalog trait                            │
│                 (crates/iceberg/src/catalog/mod.rs)              │
│                                                                  │
│  #[async_trait] pub trait Catalog: Debug + Sync + Send           │
│  ├── list_namespaces(parent) → Vec<NamespaceIdent>               │
│  ├── create_namespace(namespace, properties) → Namespace         │
│  ├── get_namespace(namespace) → Namespace                        │
│  ├── namespace_exists(namespace) → bool                          │
│  ├── update_namespace(namespace, properties)                     │
│  ├── drop_namespace(namespace)                                   │
│  ├── list_tables(namespace) → Vec<TableIdent>                    │
│  ├── create_table(namespace, creation) → Table                   │
│  ├── load_table(table) → Table                                   │
│  ├── drop_table(table)                                           │
│  ├── table_exists(table) → bool                                  │
│  ├── rename_table(src, dest)                                     │
│  ├── register_table(table, metadata_location) → Table            │
│  └── update_table(commit: TableCommit) → Table                   │
│                                                                  │
│  pub trait CatalogBuilder: Default + Debug + Send + Sync         │
│  ├── type C: Catalog                                             │
│  ├── with_storage_factory(factory) → Self                        │
│  └── load(name, props) → Result<Self::C>                         │
│                                                                  │
└──────────────┬───────────────────────────────────────────────────┘
               │
     ┌─────────┼──────────┬──────────────┬──────────────┐
     V         V          V              V              V
┌─────────┐┌────────┐┌─────────┐┌───────────┐┌──────────────┐
│  REST   ││  HMS   ││  Glue   ││ S3 Tables ││    SQL       │
│ Catalog ││Catalog ││ Catalog ││  Catalog  ││  Catalog     │
│         ││        ││         ││           ││(Postgres/    │
│REST API ││Thrift  ││AWS API  ││AWS API    ││ MySQL/SQLite)│
└─────────┘└────────┘└─────────┘└───────────┘└──────────────┘
     │         │          │              │              │
     └─────────┴──────────┴──────┬───────┴──────────────┘
                                 V
                     ┌──────────────────────┐
                     │  CatalogLoader       │   crates/catalog/loader/
                     │                      │
                     │  Dynamic dispatch:   │
                     │  load catalog by     │
                     │  name + properties   │
                     └──────────────────────┘

  Also:
  ┌───────────────┐
  │ MemoryCatalog │   crates/iceberg/src/catalog/memory/
  │ (testing)     │   In-memory catalog for unit tests
  └───────────────┘
```

---

## 6. DataFusion Integration

```
┌──────────────────────────────────────────────────────────────────────┐
│                        DataFusion SQL                                │
│                                                                      │
│  SELECT * FROM catalog.namespace.table WHERE x > 10                  │
│  INSERT INTO catalog.namespace.table VALUES (...)                    │
│                                                                      │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           V
┌──────────────────────────────────────────────────────────────────────┐
│  IcebergCatalogProvider                                              │
│  (implements DataFusion CatalogProvider)                             │
│                                                                      │
│  schema() → IcebergSchemaProvider                                    │
│    └─> table() → IcebergTableProvider                                │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           V
┌──────────────────────────────────────────────────────────────────────┐
│  IcebergTableProvider                                                │
│  (implements DataFusion TableProvider)                               │
│                                                                      │
│  ┌──────────────────┐          ┌──────────────────────┐              │
│  │ scan()           │          │ insert_into()        │              │
│  │ → ExecutionPlan  │          │ → ExecutionPlan      │              │
│  └────────┬─────────┘          └──────────┬───────────┘              │
│           │                               │                          │
└───────────┼───────────────────────────────┼──────────────────────────┘
            │                               │
  ┌─────────V──────────┐         ┌──────────V───────────┐
  │ IcebergTableScan   │         │ IcebergTableWrite     │
  │ (ExecutionPlan)    │         │ (ExecutionPlan)       │
  │                    │         │                       │
  │ execute():         │         │ execute():            │
  │  Table.scan()      │         │  TaskWriter           │
  │  → TableScan       │         │  → partitioning +     │
  │  → plan_files()    │         │    Parquet writing    │
  │  → ArrowReader     │         │                       │
  │  → RecordBatch     │         └──────────┬────────────┘
  │    Stream          │                    │
  └────────────────────┘         ┌──────────V────────────┐
                                 │ IcebergTableCommit    │
                                 │ (ExecutionPlan)       │
                                 │                       │
                                 │ execute():            │
                                 │  Transaction          │
                                 │  → fast_append()      │
                                 │  → commit(catalog)    │
                                 └──────────────────────-┘

  TaskWriter (crates/integrations/datafusion/src/task_writer.rs):
  ┌──────────────────────────────────────────┐
  │ Selects partitioning strategy:           │
  │                                          │
  │  if unpartitioned:                       │
  │    → UnpartitionedWriter                 │
  │  elif fanout_enabled:                    │
  │    → FanoutWriter (unsorted data)        │
  │  else:                                   │
  │    → ClusteredWriter (sorted data)       │
  │                                          │
  │ RecordBatchPartitionSplitter splits      │
  │ incoming batches by partition key        │
  └──────────────────────────────────────────┘
```

---

## 7. File Format Integration

```
                    ┌───────────────────────┐
                    │  Iceberg Core Layer   │
                    │                       │
                    │  ArrowReader          │  (arrow/reader.rs)
                    │  ArrowReaderBuilder   │  Parquet → Arrow RecordBatch
                    │                       │
                    │  ParquetWriter        │  (writer/file_writer/parquet_writer.rs)
                    │  ParquetWriterBuilder │  Arrow RecordBatch → Parquet
                    │                       │
                    │  RecordBatch-         │  (arrow/record_batch_transformer.rs)
                    │  Transformer          │  Column projection, partition injection
                    │                       │
                    │  ManifestWriter       │  (spec/manifest/writer.rs)
                    │  ManifestListWriter   │  Manifest → Avro files
                    └──────────┬────────────┘
                               │
                    ┌──────────V────────────┐
                    │  Rust Ecosystem       │
                    │                       │
                    │  arrow-rs             │  Arrow columnar format
                    │  parquet (crate)      │  Parquet read/write
                    │  apache-avro          │  Avro serialization (manifests)
                    │  roaring (crate)      │  RoaringTreemap (delete vectors)
                    └──────────┬────────────┘
                               │
                    ┌──────────V────────────┐
                    │  FileIO               │
                    │                       │
                    │  InputFile.reader()   │
                    │  OutputFile.writer()  │
                    └───────────────────────┘

Note: Unlike Java Iceberg, the Rust implementation:
  - Supports only Parquet as the data file format (no ORC, no Avro data files)
  - Uses arrow-rs natively (no format conversion needed — Arrow is the in-memory format)
  - Manifests are still Avro-serialized (using apache-avro crate)
  - Puffin files supported for statistics (crates/iceberg/src/puffin/)
```

---

## 8. Comparison with Java Implementation

| Aspect                    | Java Iceberg                                          | Rust Iceberg (iceberg-rust)                                |
|---------------------------|-------------------------------------------------------|------------------------------------------------------------|
| **Query engines**         | Spark, Flink, Hive, MapReduce                        | DataFusion                                                  |
| **Catalogs**              | REST, Hive, JDBC, Hadoop, Glue, Nessie               | REST, HMS, SQL, Glue, S3Tables + CatalogLoader              |
| **Data file formats**     | Parquet, ORC, Avro                                   | Parquet only                                                |
| **In-memory format**      | InternalRow (Spark), RowData (Flink)                 | Arrow RecordBatch (native)                                  |
| **Storage backends**      | S3FileIO, GCSFileIO, ADLSFileIO, HadoopFileIO        | OpenDAL (S3, GCS, ADLS, HDFS, etc.), LocalFs, Memory        |
| **Write operations**      | AppendFiles, OverwriteFiles, RewriteFiles, RowDelta  | FastAppend only                                             |
| **Delete strategies**     | CoW + MoR (position deletes, equality deletes, DVs)  | MoR read support (position + equality deletes + DV read)    |
| **Compaction**            | RewriteDataFiles (BinPack, Sort, ZOrder)             | Not yet implemented                                         |
| **UPDATE / MERGE**        | CoW + MoR via Spark                                  | Not yet implemented                                         |
| **Manifest format**       | Avro                                                 | Avro                                                        |
| **Format versions**       | V1, V2, V3                                           | V1, V2, V3                                                  |
| **Serialization**         | Java Serializable, Kryo                              | serde + typetag (for Storage trait objects)                 |
| **Async model**           | Java threads, ExecutorService                        | Tokio async/await, futures streams                          |
