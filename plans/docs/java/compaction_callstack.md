# Iceberg Compaction (RewriteDataFiles) — Call Stack

This document traces the complete compaction flow from user invocation through file planning, rewriting, and metadata commit.

---

## 1. High-Level Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│  USER INVOCATION                                                     │
│                                                                      │
│  Spark SQL:  CALL catalog.system.rewrite_data_files('db.table')      │
│  Java API:   SparkActions.get(spark).rewriteDataFiles(table)         │
│                .filter(expr).binPack().execute()                     │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                    ┌───────────▼─────────────────┐
                    │  1. INITIALIZATION          │   DRIVER
                    │                             │
                    │  RewriteDataFilesSparkAction│
                    │    .execute()               │
                    │       │                     │
                    │       ├─► init(snapshotId)  │
                    │       ├─► select strategy   │
                    │       │   (BinPack/Sort/    │
                    │       │    ZOrder)          │
                    │       └─► validate options  │
                    └───────────┬─────────────────┘
                                │
                    ┌───────────▼─────────────────┐
                    │  2. FILE PLANNING           │   DRIVER
                    │                             │
                    │  BinPackRewriteFilePlanner  │
                    │  (or SparkShufflingData-    │
                    │   RewritePlanner)           │
                    │       │                     │
                    │       ├─► scan table files  │
                    │       ├─► filter by size/   │
                    │       │   delete thresholds │
                    │       ├─► group by          │
                    │       │   partition         │
                    │       ├─► BinPacking        │
                    │       │   algorithm         │
                    │       └─► FileRewritePlan   │
                    │           (groups of files) │
                    └───────────┬─────────────────┘
                                │
                    ┌───────────▼─────────────────┐
                    │  3. FILE REWRITING          │   SPARK JOBS
                    │                             │
                    │  For each RewriteFileGroup  │
                    │  (parallel via              │
                    │   ExecutorService):         │
                    │                             │
                    │  ┌──────────────────────┐   │
                    │  │ SparkBinPackFile-    │   │
                    │  │ RewriteRunner        │   │
                    │  │   .rewrite(group)    │   │
                    │  │                      │   │
                    │  │  READ:               │   │
                    │  │   spark.read         │   │
                    │  │    .format("iceberg")│   │
                    │  │    .load(groupId)    │   │
                    │  │                      │   │
                    │  │  WRITE:              │   │
                    │  │   df.write           │   │
                    │  │    .format("iceberg")│   │
                    │  │    .mode("append")   │   │
                    │  │    .save(groupId)    │   │
                    │  └──────────────────────┘   │
                    └───────────┬─────────────────┘
                                │
                    ┌───────────▼─────────────────┐
                    │  4. METADATA COMMIT         │   DRIVER
                    │                             │
                    │  RewriteDataFilesCommit-    │
                    │  Manager                    │
                    │    .commitFileGroups()      │
                    │       │                     │
                    │       ▼                     │
                    │  table.newRewrite()         │
                    │    .deleteFile(old)         │
                    │    .addFile(new)            │
                    │    .commit()                │
                    │       │                     │
                    │       ▼                     │
                    │  NEW SNAPSHOT               │
                    └────────────────────────────-┘
```

---

## 2. Detailed Call Stack

### Phase 1: Entry Point & Initialization

```
CALL catalog.system.rewrite_data_files(                          [Spark SQL]
  table => 'db.table',
  strategy => 'binpack',
  where => 'date > "2024-01-01"'
)

RewriteDataFilesProcedure.call(args)                             [spark/procedures]
  │
  └─► SparkActions.get(spark)
        .rewriteDataFiles(table)                                 [spark/actions]
          └─► new RewriteDataFilesSparkAction(spark, table)

RewriteDataFilesSparkAction                                      [spark/actions]
  │
  ├─► .filter(expression)          optional: limit to partitions
  ├─► .binPack()                   strategy: SparkBinPackFileRewriteRunner
  │   .sort(sortOrder)             strategy: SparkSortFileRewriteRunner
  │   .zOrder(col1, col2, ...)     strategy: SparkZOrderFileRewriteRunner
  │
  └─► .execute()
        │
        ├─► init(startingSnapshotId)
        │     ├─► select runner (default: BinPack)
        │     ├─► create planner:
        │     │     BinPackRewriteFilePlanner(table, filter, snapshotId)
        │     │     (or SparkShufflingDataRewritePlanner for Sort/ZOrder)
        │     └─► validateAndInitOptions()
        │
        └─► continue to planning phase...
```

### Phase 2: File Planning

```
RewriteDataFilesSparkAction.execute() (continued)               [spark/actions]
  │
  └─► FileRewritePlan plan = planner.plan()

BinPackRewriteFilePlanner.plan()                                 [core/actions]
  extends SizeBasedFileRewritePlanner
  │
  ├─► Scan table for all files:
  │     table.newScan()
  │       .filter(userFilter)
  │       .useSnapshot(startingSnapshotId)
  │       .planFiles()
  │         └─► CloseableIterable<FileScanTask>
  │
  ├─► Group tasks by partition:
  │     groupTasksByPartition(fileScanTasks)
  │       └─► Map<StructLike, List<FileScanTask>>
  │
  ├─► For each partition, select files needing rewrite:
  │     │
  │     ├─► Size-based selection:
  │     │     file.fileSizeInBytes < MIN_FILE_SIZE (75% of target)
  │     │     file.fileSizeInBytes > MAX_FILE_SIZE (180% of target)
  │     │
  │     ├─► Delete-based selection:
  │     │     file.deleteFileCount >= DELETE_FILE_THRESHOLD
  │     │     deleteRatio >= DELETE_RATIO_THRESHOLD (0.3)
  │     │
  │     └─► Skip if:
  │           group has < MIN_INPUT_FILES (5)
  │           AND rewrite wouldn't reduce file count
  │
  ├─► BinPacking algorithm:                                      [core/util/BinPacking]
  │     BinPacking.ListPacker(maxGroupSize)
  │       .pack(candidateFiles, file -> file.sizeInBytes)
  │         └─► First-fit-decreasing bin packing
  │             ├─► sort files by size (largest first)
  │             ├─► assign each file to first bin that fits
  │             └─► returns List<List<FileScanTask>>
  │
  └─► Create RewriteFileGroup for each bin:
        RewriteFileGroup
          ├─ info: FileGroupInfo(globalIdx, partitionIdx, partition)
          ├─ fileScanTasks: List<FileScanTask>
          ├─ outputSpecId: int
          ├─ maxOutputFileSize: long
          └─ inputSplitSize: long

        └─► return FileRewritePlan(groups)
```

### Phase 3: File Rewriting (Per Group)

```
RewriteDataFilesSparkAction.execute() (continued)               [spark/actions]
  │
  ├─► doExecute(plan, commitManager)
  │     │
  │     ├─► create ExecutorService(maxConcurrentRewrites)
  │     │
  │     └─► Tasks.foreach(plan.groups())
  │           .executeWith(rewriteService)
  │           .run(group → rewriteFiles(plan, group))

rewriteFiles(plan, RewriteFileGroup group)                       [spark/actions]
  │
  └─► runner.rewrite(group)

SparkBinPackFileRewriteRunner.rewrite(group)                     [spark/actions]
  extends SparkDataFileRewriteRunner
  │
  ├─► String groupId = UUID.randomUUID()
  │
  ├─► Stage files for coordinated read:
  │     tableCache.add(groupId, table)
  │     taskSetManager.stageTasks(table, groupId, fileScanTasks)
  │
  ├─► doRewrite(groupId, group)
  │     │
  │     ├─► READ phase (Spark job):
  │     │     spark.read()
  │     │       .format("iceberg")
  │     │       .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
  │     │       .option(SparkReadOptions.SPLIT_SIZE, inputSplitSize)
  │     │       .option(SparkReadOptions.FILE_OPEN_COST, "0")
  │     │       .load(groupId)
  │     │         │
  │     │         └─► SparkStagedScanBuilder (reads pre-staged tasks)
  │     │               └─► reads original data files via normal read path
  │     │
  │     └─► WRITE phase (Spark job):
  │           scanDF.write()
  │             .format("iceberg")
  │             .option(REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
  │             .option(TARGET_FILE_SIZE_BYTES, maxOutputFileSize)
  │             .option(DISTRIBUTION_MODE, "none")     ◄── BinPack: no shuffle
  │             .mode("append")
  │             .save(groupId)
  │               │
  │               └─► normal write path → new Parquet files
  │                   (files registered via FileRewriteCoordinator,
  │                    NOT committed to table yet)
  │
  ├─► Fetch new files:
  │     coordinator.fetchNewFiles(table, groupId)
  │       └─► Set<DataFile> addedFiles
  │
  ├─► group.setOutputFiles(addedFiles)
  │
  └─► Cleanup:
        tableCache.remove(groupId)
        taskSetManager.removeTasks(groupId)
```

**Sort and ZOrder runners differ in the WRITE phase:**

```
SparkSortFileRewriteRunner.doRewrite()                           [spark/actions]
  └─► WRITE phase:
        .option(DISTRIBUTION_MODE, "range")        ◄── Sort: range-partition shuffle
        .option(SORT_ORDER, userSortOrder)
        .save(groupId)

SparkZOrderFileRewriteRunner.doRewrite()                         [spark/actions]
  └─► WRITE phase:
        .option(DISTRIBUTION_MODE, "range")
        Z-order interleaving on specified columns
        .save(groupId)
```

### Phase 4: Metadata Commit

```
RewriteDataFilesSparkAction.execute() (continued)               [spark/actions]
  │
  └─► commitManager.commitFileGroups(allRewrittenGroups)

RewriteDataFilesCommitManager.commitFileGroups(groups)           [core/actions]
  │
  ├─► Aggregate files from all groups:
  │     for each RewriteFileGroup:
  │       rewrittenDataFiles.addAll(group.rewrittenFiles())      old files to remove
  │       addedDataFiles.addAll(group.addedFiles())              new files to add
  │       danglingDVs.addAll(group.danglingDVs())                orphaned delete vectors
  │
  ├─► Create RewriteFiles transaction:
  │     RewriteFiles rewrite = table.newRewrite()                [api/Table]
  │       └─► new BaseRewriteFiles(...)                          [core/BaseRewriteFiles]
  │
  ├─► Configure transaction:
  │     rewrite.validateFromSnapshot(startingSnapshotId)
  │     rewrite.dataSequenceNumber(sequenceNumber)               (if useStartingSeqNum)
  │
  ├─► Remove old files:
  │     for each DataFile in rewrittenDataFiles:
  │       rewrite.deleteFile(dataFile)
  │
  ├─► Add new files:
  │     for each DataFile in addedDataFiles:
  │       rewrite.addFile(dataFile)
  │
  ├─► Remove dangling deletes:
  │     for each DeleteFile in danglingDVs:
  │       rewrite.deleteFile(deleteFile)
  │
  ├─► Set snapshot properties:
  │     rewrite.set("rewrite-job-id", jobId)
  │
  └─► rewrite.commit()
        └─► SnapshotProducer.commit()                           [core/SnapshotProducer]
              │
              ├─► Validate: files-to-delete still exist in current snapshot
              │     (if concurrent writes added/removed files, may conflict)
              │
              ├─► Write new manifests:
              │     ├─ manifest with DELETED entries (old files)
              │     └─ manifest with ADDED entries (new files)
              │
              ├─► Write manifest list
              │
              ├─► Create Snapshot:
              │     operation = "replace"
              │     summary = {
              │       "added-data-files": N,
              │       "deleted-data-files": M,
              │       "added-records": X,
              │       "deleted-records": Y
              │     }
              │
              └─► TableOperations.commit(base, updated)
                    └─► atomic CAS on metadata.json
                          (retry on conflict)
```

### Phase 5: Post-Compaction (Optional)

```
[After compaction completes]

ExpireSnapshots (removes old snapshots):
  SparkActions.get(spark)
    .expireSnapshots(table)
    .expireOlderThan(timestampMillis)
    .execute()
      └─► removes unreferenced snapshots
          └─► allows GC of old data files replaced by compaction

RemoveDanglingDeletes (if enabled in RewriteDataFiles):
  └─► removes delete files that no longer reference any live data files
```

---

## 3. Strategy Comparison

```
┌──────────────┬─────────────────┬──────────────────┬─────────────────┐
│              │    BIN-PACK     │     SORT         │    Z-ORDER      │
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Goal         │ Consolidate     │ Consolidate +    │ Consolidate +   │
│              │ small files     │ sort data        │ multi-dim sort  │
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Shuffle      │ None            │ Range partition  │ Range partition │
│              │                 │ + sort           │ + z-order       │
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
│ Runner class │ SparkBinPack-   │ SparkSort-       │ SparkZOrder-    │
│              │ FileRewrite-    │ FileRewrite-     │ FileRewrite-    │
│              │ Runner          │ Runner           │ Runner          │
├──────────────┼─────────────────┼──────────────────┼─────────────────┤
│ Planner      │ BinPackRewrite- │ SparkShuffling-  │ SparkShuffling- │
│              │ FilePlanner     │ DataRewrite-     │ DataRewrite-    │
│              │                 │ Planner          │ Planner         │
└──────────────┴─────────────────┴──────────────────┴─────────────────┘
```

---

## 4. Key Classes Reference

| Step         | Class                              | Module           | Key Method                              |
|--------------|------------------------------------|------------------|-----------------------------------------|
| SQL entry    | `RewriteDataFilesProcedure`        | spark/procedures | `call()`                                |
| API entry    | `SparkActions`                     | spark/actions    | `rewriteDataFiles()`                    |
| Orchestrator | `RewriteDataFilesSparkAction`      | spark/actions    | `execute()`                             |
| Planner      | `BinPackRewriteFilePlanner`        | core/actions     | `plan()`                                |
| Planner      | `SparkShufflingDataRewritePlanner` | spark/actions    | `plan()` (sort/zorder)                  |
| Size logic   | `SizeBasedFileRewritePlanner`      | core/actions     | file selection thresholds               |
| Bin packing  | `BinPacking.ListPacker`            | core/util        | `pack()`                                |
| Plan result  | `FileRewritePlan`                  | core/actions     | `groups()`                              |
| File group   | `RewriteFileGroup`                 | core/actions     | files + output info                     |
| BinPack run  | `SparkBinPackFileRewriteRunner`    | spark/actions    | `rewrite()`, `doRewrite()`              |
| Sort run     | `SparkSortFileRewriteRunner`       | spark/actions    | `rewrite()`, `doRewrite()`              |
| ZOrder run   | `SparkZOrderFileRewriteRunner`     | spark/actions    | `rewrite()`, `doRewrite()`              |
| Staging      | `ScanTaskSetManager`               | spark/source     | `stageTasks()`                          |
| Staging      | `SparkTableCache`                  | spark/source     | `add()`, `remove()`                     |
| Coordination | `FileRewriteCoordinator`           | spark/source     | `fetchNewFiles()`                       |
| Commit mgr   | `RewriteDataFilesCommitManager`    | core/actions     | `commitFileGroups()`                    |
| Rewrite op   | `BaseRewriteFiles`                 | core             | `deleteFile()`, `addFile()`, `commit()` |
| Snapshot     | `SnapshotProducer`                 | core             | `commit()` → new snapshot               |

---

## 5. Configuration Options

| Option                               | Default          | Purpose                                        |
|--------------------------------------|------------------|------------------------------------------------|
| `target-file-size-bytes`             | from table props | Target output file size                        |
| `min-file-size-bytes`                | 75% of target    | Files smaller than this are candidates         |
| `max-file-size-bytes`                | 180% of target   | Files larger than this are candidates          |
| `min-input-files`                    | 5                | Min files in a group to justify rewrite        |
| `max-file-group-size-bytes`          | 100 GB           | Max total size per rewrite group               |
| `max-concurrent-file-group-rewrites` | 5                | Parallel rewrite groups                        |
| `partial-progress.enabled`           | false            | Commit each group independently                |
| `partial-progress.max-commits`       | 10               | Max commits if partial progress                |
| `use-starting-sequence-number`       | true             | Preserve sequence number for deletes           |
| `remove-dangling-deletes`            | false            | Clean orphaned delete files                    |
| `rewrite-job-order`                  | none             | Group ordering: bytes-asc/desc, files-asc/desc |
| `delete-file-threshold`              | MAX_INT          | Files with N+ delete files are rewritten       |
| `delete-ratio-threshold`             | 0.3              | Files with 30%+ deleted rows are rewritten     |
