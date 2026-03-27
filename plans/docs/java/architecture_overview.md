# Apache Iceberg Architecture Overview

This document provides a high-level architecture overview of Apache Iceberg, excluding the Native Layer modules.

---

## 1. Layered Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                        QUERY ENGINES                               │
│   ┌───────────┐  ┌───────────┐  ┌────────────┐  ┌──────────────┐   │
│   │  Spark    │  │  Flink    │  │  MapReduce │  │  Hive        │   │
│   │  3.4/3.5  │  │  1.19/    │  │            │  │              │   │
│   │  /4.0     │  │  1.20/2.0 │  │            │  │              │   │
│   └─────┬─────┘  └─────┬─────┘  └──────┬─────┘  └──────┬───────┘   │
│         │              │               │               │           │
└─────────┼──────────────┼───────────────┼───────────────┼───────────┘
          │              │               │               │
┌─────────▼──────────────▼───────────────▼───────────────▼───────────┐
│                   ENGINE CONNECTORS                                │
│   ┌───────────┐  ┌───────────┐  ┌────────────┐  ┌──────────────┐   │
│   │ spark/    │  │ flink/    │  │ mr/        │  │ hive-        │   │
│   │  source/  │  │           │  │            │  │ metastore/   │   │
│   └─────┬─────┘  └─────┬─────┘  └──────┬─────┘  └──────┬───────┘   │
│         │              │               │               │           │
└─────────┼──────────────┼───────────────┼───────────────┼───────────┘
          │              │               │               │
          └──────────────┴───────┬───────┴───────────────┘
                                 │
┌────────────────────────────────▼───────────────────────────────────┐
│                     ICEBERG API (api/)                             │
│                                                                    │
│   Table, Schema, PartitionSpec, SortOrder, Snapshot                │
│   TableScan, AppendFiles, RewriteFiles, DeleteFiles                │
│   Catalog, Expression, Types, Transforms                           │
│                                                                    │
└────────────────────────────────┬───────────────────────────────────┘
                                 │
┌────────────────────────────────▼───────────────────────────────────┐
│                   ICEBERG CORE (core/)                             │
│                                                                    │
│   TableMetadata, BaseTable, BaseTableScan, ManifestGroup           │
│   SnapshotProducer, FastAppend, MergeAppend, BaseRewriteFiles      │
│   ManifestReader, ManifestWriter, ManifestListWriter               │
│   TableMetadataParser, SchemaParser                                │
│                                                                    │
└────────────┬───────────────────┬───────────────────┬───────────────┘
             │                   │                   │
┌────────────▼─────┐  ┌──────────▼────────┐  ┌───────▼──────────────┐
│ FILE FORMATS     │  │ DATA MODULE       │  │ COMMON UTILITIES     │
│                  │  │ (data/)           │  │ (common/)            │
│ ┌─────────────┐  │  │                   │  │                      │
│ │ parquet/    │  │  │ GenericReader     │  │ Shared utilities     │
│ │ Parquet.java│  │  │ GenericWriter     │  │ across modules       │
│ │ ParquetRdr  │  │  │ BaseFileWriter-   │  │                      │
│ │ ParquetWtr  │  │  │   Factory         │  │                      │
│ ├─────────────┤  │  │                   │  │                      │
│ │ orc/        │  │  │                   │  │                      │
│ ├─────────────┤  │  │                   │  │                      │
│ │ arrow/      │  │  │                   │  │                      │
│ └─────────────┘  │  │                   │  │                      │
└──────────────────┘  └───────────────────┘  └──────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────────────┐
│                     FILE I/O LAYER                                 │
│                                                                    │
│   InputFile, OutputFile, FileIO                                    │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │
│   │ aws/     │  │ azure/   │  │ gcp/     │  │ hadoop (local/   │   │
│   │ S3FileIO │  │ ADLSFile │  │ GCSFile  │  │  HDFS)           │   │
│   │          │  │ IO       │  │ IO       │  │                  │   │
│   └──────────┘  └──────────┘  └──────────┘  └──────────────────┘   │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
             │
┌────────────▼───────────────────────────────────────────────────────┐
│                        STORAGE                                     │
│    S3  |  ADLS  |  GCS  |  HDFS  |  Local FS                       │
└────────────────────────────────────────────────────────────────────┘
```

---

## 2. Module Structure

```
iceberg/
├── api/                    Public API interfaces and contracts
├── core/                   Reference implementation of the API
├── common/                 Shared utilities across modules
│
├── parquet/                Parquet file format reader/writer
├── orc/                    ORC file format support
├── arrow/                  Apache Arrow columnar format integration
├── data/                   Direct JVM data read/write, BaseFileWriterFactory
│
├── spark/                  Spark DataSourceV2 integration
│   ├── v3.4/
│   ├── v3.5/               (primary development target)
│   └── v4.0/
│
├── flink/                  Flink integration
│   ├── v1.19/
│   ├── v1.20/
│   └── v2.0/
│
├── mr/                     Hadoop MapReduce InputFormat
├── hive-metastore/         Hive metastore Thrift client
│
├── aws/                    S3 / Glue catalog integration
├── azure/                  ADLS integration
├── gcp/                    GCS integration
├── aliyun/                 Alibaba Cloud OSS integration
├── dell/                   Dell ECS integration
│
├── kafka-connect/          Kafka Connect integration
├── open-api/               REST catalog OpenAPI spec
│
├── format/                 Iceberg format specification
└── docs/                   Documentation site
```

---

## 3. Table Metadata Structure

```
                    ┌─────────────────────────┐
                    │   metadata.json (v3)    │
                    │                         │
                    │  format-version: 2      │
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
                    ▼
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
                    ▼
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
                    ▼
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
                    ▼
          ┌──────────────────────┐
          │  DataFile            │
          │                      │
          │  file-path           │    Actual data files:
          │  file-format         │    - Parquet (.parquet)
          │  partition           │    - ORC (.orc)
          │  record-count        │    - Avro (.avro)
          │  file-size-bytes     │
          │  column-sizes{}      │    Per-column stats:
          │  value-counts{}      │    - null counts
          │  null-value-counts{} │    - lower/upper bounds
          │  lower-bounds{}      │    - NaN counts
          │  upper-bounds{}      │
          └──────────────────────┘
```

---

## 4. Key API Interfaces

```
                        ┌──────────────┐
                        │   Catalog    │
                        │              │
                        │ loadTable()  │
                        │ createTable()│
                        │ dropTable()  │
                        └──────┬───────┘
                               │ returns
                               ▼
                        ┌──────────────┐
                        │    Table     │
                        │              │
                        │ schema()     │──────► Schema ──► StructType ──► NestedField[]
                        │ spec()       │──────► PartitionSpec ──► PartitionField[]
                        │ sortOrder()  │──────► SortOrder ──► SortField[]
                        │ currentSnap()│──────► Snapshot
                        │ properties() │
                        │ io()         │──────► FileIO
                        │ location()   │
                        │              │
                        │ newScan()  ──┼──┐    READS
                        │              │  │
                        │ newAppend()──┼──┼─┐  WRITES / MUTATIONS
                        │ newOverwrite │  │ │
                        │ newRewrite() │  │ │
                        │ newDelete()  │  │ │
                        │ newRowDelta  │  │ │
                        └──────────────┘  │ │
                               ┌──────────┘ │
                               ▼            │
                     ┌──────────────────┐   │
                     │   TableScan      │   │
                     │                  │   │
                     │ select(columns)  │   │
                     │ filter(expr)     │   │
                     │ planFiles()    ──┼───┼──► CloseableIterable<FileScanTask>
                     │ planTasks()    ──┼───┼──► CloseableIterable<CombinedScanTask>
                     └──────────────────┘   │
                                            │
                               ┌────────────┘
                               ▼
                     ┌──────────────────────┐
                     │ SnapshotUpdate<T>    │       (common base for mutations)
                     │                      │
                     │ set(prop, value)     │
                     │ commit()             │──────► atomic metadata update
                     │                      │
                     ├──────────────────────┤
                     │ AppendFiles          │  add new data files
                     │ OverwriteFiles       │  replace data files (filter-based)
                     │ RewriteFiles         │  swap old files for new (compaction)
                     │ DeleteFiles          │  delete data files by expression
                     │ RowDelta             │  add data + delete files (MoR)
                     │ ReplacePartitions    │  dynamic partition overwrite
                     │ ExpireSnapshots      │  remove old snapshots
                     │ RewriteManifests     │  optimize manifest files
                     └──────────────────────┘
```

---

## 5. Catalog Subsystem

```
┌──────────────────────────────────────────────────────────────────┐
│                     Spark Catalog Layer                          │
│                                                                  │
│  ┌──────────────────┐     ┌──────────────────────────┐           │
│  │ SparkCatalog     │     │ SparkSessionCatalog      │           │
│  │ (TableCatalog)   │     │ (wraps built-in catalog) │           │
│  └────────┬─────────┘     └───────────┬──────────────┘           │
│           │                           │                          │
│           └─────────┬─────────────────┘                          │
│                     │ delegates to                               │
└─────────────────────┼────────────────────────────────────────────┘
                      │
┌─────────────────────▼────────────────────────────────────────────┐
│                 Iceberg Catalog (api/)                           │
│                                                                  │
│  interface Catalog                                               │
│  ├── loadTable(TableIdentifier) → Table                          │
│  ├── createTable(ident, schema, spec, location, props) → Table   │
│  ├── dropTable(ident, purge)                                     │
│  ├── renameTable(from, to)                                       │
│  └── listTables(namespace) → List<TableIdentifier>               │
│                                                                  │
│  interface SupportsNamespaces                                    │
│  ├── createNamespace(namespace, meta)                            │
│  ├── dropNamespace(namespace)                                    │
│  └── listNamespaces() → List<Namespace>                          │
│                                                                  │
└─────────────────────┬────────────────────────────────────────────┘
                      │
        ┌─────────────┼──────────────┬────────────────┐
        ▼             ▼              ▼                ▼
  ┌────────────┐ ┌───────────┐ ┌────────────┐ ┌──────────────┐
  │ HiveCatalog│ │RESTCatalog│ │JdbcCatalog │ │HadoopCatalog │
  │            │ │           │ │            │ │(no metastore)│
  │ Hive       │ │ REST API  │ │ JDBC       │ │ filesystem   │
  │ Metastore  │ │ server    │ │ database   │ │ based        │
  └────────────┘ └───────────┘ └────────────┘ └──────────────┘
       │             │             │                │
       └─────────────┴──────┬──────┴────────────────┘
                            ▼
                    ┌────────────────┐
                    │ TableOperations│   Atomic metadata updates
                    │                │   (CAS on metadata.json)
                    │ current()      │
                    │ refresh()      │
                    │ commit(base,   │
                    │   updated)     │
                    └────────────────┘
```

---

## 6. Spark DataSourceV2 Integration

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Spark SQL                                     │
│                                                                      │
│  SELECT * FROM catalog.db.table WHERE x > 10                         │
│  INSERT INTO catalog.db.table VALUES (...)                           │
│                                                                      │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│  IcebergSource (TableProvider, SupportsCatalogOptions)               │
│  Registered via META-INF/services as "iceberg" format                │
│                                                                      │
│  getTable() → SparkTable                                             │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│  SparkTable                                                          │
│  Implements: SupportsRead, SupportsWrite, SupportsDeleteV2,          │
│              SupportsRowLevelOperations, SupportsMetadataColumns     │
│                                                                      │
│  ┌──────────────────┐          ┌──────────────────────┐              │
│  │ newScanBuilder() │          │ newWriteBuilder()    │              │
│  └────────┬─────────┘          └──────────┬───────────┘              │
│           │                               │                          │
└───────────┼───────────────────────────────┼──────────────────────────┘
            │                               │
  ┌─────────▼──────────┐         ┌──────────▼───────────┐
  │ SparkScanBuilder   │         │ SparkWriteBuilder    │
  │ (filter/col push)  │         │ (append/overwrite)   │
  └─────────┬──────────┘         └──────────┬───────────┘
            │                               │
  ┌─────────▼──────────┐         ┌──────────▼───────────┐
  │ SparkBatchQueryScan│         │ SparkWrite           │
  │ (logical scan)     │         │ (physical write)     │
  └─────────┬──────────┘         └──────────┬───────────┘
            │                               │
  ┌─────────▼──────────┐         ┌──────────▼───────────┐
  │ SparkBatch         │         │ BatchAppend /        │
  │ (partition planning│         │ DynamicOverwrite     │
  │  + reader factory) │         │ (commit strategy)    │
  └─────────┬──────────┘         └──────────┬───────────┘
            │                               │
     ┌──────▼──────┐                 ┌──────▼──────┐
     │  EXECUTORS  │                 │  EXECUTORS  │
     │ RowDataRdr  │                 │ DataWriter  │
     │ BatchDtaRdr │                 │ FileWriter  │
     └─────────────┘                 └─────────────┘
```

---

## 7. File Format Integration

```
                    ┌───────────────────────┐
                    │  Engine Layer         │
                    │                       │
                    │  SparkParquetReaders  │  (InternalRow <-> Parquet)
                    │  SparkParquetWriters  │
                    │  SparkOrcReaders      │
                    │  SparkAvroReaders     │
                    └──────────┬────────────┘
                               │
                    ┌──────────▼────────────┐
                    │  Format Layer         │
                    │                       │
                    │  Parquet.read()       │──► ReadBuilder ──► ParquetReader
                    │  Parquet.writeData()  │──► DataWriteBuilder ──► ParquetWriter
                    │  ORC.read()           │──► ReadBuilder ──► OrcIterable
                    │  ORC.write()          │──► WriteBuilder ──► OrcFileAppender
                    │                       │
                    │  ParquetValueReader   │  (column-level decode)
                    │  ParquetValueWriter   │  (column-level encode)
                    └──────────┬────────────┘
                               │
                    ┌──────────▼────────────┐
                    │  Apache Libraries     │
                    │                       │
                    │  parquet-mr           │  (ParquetFileReader, ParquetFileWriter)
                    │  orc-core             │
                    │  avro                 │
                    └──────────┬────────────┘
                               │
                    ┌──────────▼────────────┐
                    │  FileIO               │
                    │                       │
                    │  InputFile.newStream()│
                    │  OutputFile.create()  │
                    └───────────────────────┘
```
