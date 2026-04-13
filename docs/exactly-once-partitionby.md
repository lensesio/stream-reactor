# Exactly-Once Delivery with Custom Partitioning (PARTITIONBY)

This document explains how the cloud sink connectors (S3, GCS, Azure Data Lake) achieve exactly-once delivery semantics when records are repartitioned into cloud storage using the `PARTITIONBY` clause, and the architectural issues that existed before the two-tier lock system was introduced.

---

## Background: How Offset Tracking Works

Kafka Connect sink connectors consume records from Kafka topic partitions. To provide exactly-once guarantees, the connector must track which offsets have been durably written to cloud storage so that:

1. **On crash recovery**, it knows where to resume without re-writing records that are already in storage (avoiding duplicates).
2. **During normal operation**, it tells Kafka Connect which offsets are safe to commit to the consumer group, so that records are never skipped (avoiding data loss).

The connector uses **lock files** stored in cloud storage (e.g., `.indexes/<connector>/.locks/<topic>/<partition>.lock`) to persist this tracking state. The `.indexes` directory name is configurable via the `connect.<prefix>.indexes.name` connector property (default: `.indexes`). Each lock file is a small JSON document containing:

- `owner` -- a random UUID generated each time the task starts. Used purely for observability (inspecting who last wrote the lock); not used for any decision-making logic.
- `committedOffset` -- the highest Kafka offset whose record has been durably written to storage.
- `pendingState` -- (optional) in-flight operations that need to be completed or rolled back on restart.

Lock files are written atomically using **eTag-based conditional writes** (sometimes called "generation" or "precondition" writes). A write either fully succeeds with a new eTag, or fully fails leaving the previous value intact. There is no partial or corrupted state.

---

## What PARTITIONBY Does

Without `PARTITIONBY`, there is a one-to-one mapping between a Kafka partition and a `Writer` instance. A single writer handles all records from its partition, writes them sequentially to a single output file, and updates a single lock file.

With `PARTITIONBY`, the connector repartitions records based on field values in the record. For example:

```sql
INSERT INTO s3bucket:output SELECT * FROM my_topic PARTITIONBY _value.date
```

This fans out a single Kafka partition into **multiple output paths** (one per unique value of `date`). Each unique partition value gets its own `Writer`:

```
Kafka Partition 0
  ├─ Writer A (date=2024-01-01) → output/date=2024-01-01/000000000104.parquet
  ├─ Writer B (date=2024-01-02) → output/date=2024-01-02/000000000103.parquet
  └─ Writer C (date=2024-01-03) → output/date=2024-01-03/000000000099.parquet
```

Each writer accumulates records until a flush threshold is met (record count, file size, or time interval), then commits its batch to cloud storage and updates the lock file.

---

## The Problem: Single Lock File per Kafka Partition

### Architecture before the fix

Before the two-tier lock system, there was **one lock file per Kafka partition** regardless of how many writers existed:

```
.indexes/<connector>/.locks/<topic>/0.lock    ← shared by ALL writers for partition 0
```

Every writer for that Kafka partition called `indexManager.update` on the **same** lock file. The lock file stored a single `committedOffset`. Since writers handle disjoint, interleaved subsets of offsets, they **overwrote each other's progress**.

### Issue 1: Data Duplication

Consider Kafka partition 0 with two writers, each handling different date values. The offsets are interleaved across the partition because records arrive in Kafka order, not partitioned by date:

```
Offset 100  →  date=2024-01-01  →  Writer A
Offset 101  →  date=2024-01-02  →  Writer B
Offset 102  →  date=2024-01-01  →  Writer A
Offset 103  →  date=2024-01-02  →  Writer B
Offset 104  →  date=2024-01-01  →  Writer A
```

Writer A processes offsets {100, 102, 104} and commits first. The lock file is updated:

```json
{ "committedOffset": 104 }
```

Writer B processes offsets {101, 103} and commits **after** Writer A. The lock file is overwritten:

```json
{ "committedOffset": 103 }
```

The lock file now says 103, but Writer A already durably wrote records up to 104. If the process crashes and restarts:

1. The lock file is read → seek to offset 103.
2. Offset 104 is replayed. Writer A already wrote this record.
3. **Result: offset 104 is duplicated in the output.**

The root cause is that Writer B's lower committed offset **regressed** the lock file, erasing Writer A's higher progress. The commit order between writers is non-deterministic (depends on which writer's flush threshold is reached first), so this race is inherent to the single-lock design.

### Issue 2: Data Loss

Consider the reverse timing with a slightly different offset distribution:

```
Offset 100  →  date=2024-01-01  →  Writer A  (buffered, not yet committed)
Offset 101  →  date=2024-01-02  →  Writer B
Offset 102  →  date=2024-01-01  →  Writer A  (buffered, not yet committed)
Offset 103  →  date=2024-01-02  →  Writer B
Offset 104  →  date=2024-01-01  →  Writer A  (buffered, not yet committed)
Offset 105  →  date=2024-01-02  →  Writer B
```

Writer B reaches its flush threshold first and commits offsets {101, 103, 105}. Lock file: `committedOffset = 105`.

Writer A has offsets {100, 102, 104} **buffered in memory** but has not yet reached its flush threshold.

When Kafka Connect calls `preCommit`, the old code computed the offset to report as `max(committedOffset across all writers) + 1`. It found Writer B's committed offset 105 and returned **106** to Kafka Connect. Kafka committed consumer offset 106 to the consumer group.

Now the process crashes before Writer A commits:

1. On restart, Kafka seeks to offset 106 (the committed consumer offset).
2. Offsets 100, 102, 104 (Writer A's buffered data) are **never replayed** -- Kafka believes they were already processed.
3. **Result: offsets 100, 102, 104 are permanently lost.**

The root cause is that `preCommit` reported the **maximum** committed offset across writers, but other writers had uncommitted data at **lower** offsets. Kafka advanced past those offsets, and the buffered data was never delivered again -- whether the task stopped due to a crash (buffered data vanishes with the process) or a graceful rebalance (`close()` discards staging files). The damage is done the moment Kafka Connect commits the too-high consumer offset; how the task subsequently stops is irrelevant.

---

## The Solution: Two-Tier Lock Architecture

The fix introduces a **two-tier lock system** consisting of a master lock and per-writer granular locks.

### Lock file layout

```
<indexes-dir>/<connector>/.locks/<topic>/
  ├─ 0.lock                              ← Master lock (one per Kafka partition)
  └─ 0/                                  ← Granular lock directory
      ├─ date%3D2024-01-01.lock          ← Granular lock for Writer A
      ├─ date%3D2024-01-02.lock          ← Granular lock for Writer B
      └─ date%3D2024-01-03.lock          ← Granular lock for Writer C
```

Where `<indexes-dir>` is the value of `connect.<prefix>.indexes.name` (default: `.indexes`).

- **Master lock** (`<partition>.lock`): A single lock file per Kafka partition that stores the `globalSafeOffset`. This is what the consumer offset commit is based on, and what old code reads for backward compatibility.
- **Granular locks** (`<partition>/<partitionKey>.lock`): One lock file per writer, identified by a sanitized, deterministic partition key derived from the `PARTITIONBY` field values. Each writer tracks its own `committedOffset` independently.

Partition keys are derived by sorting field names alphabetically and URL-encoding both the field name and value to avoid path collisions. Fields and values are joined with a literal `=` separator, and multiple fields are separated by `_`:

```
PARTITIONBY _value.date, _value.hour
→ partitionKey = "date=2024-01-01_hour=12"
```

The structural `=` between field name and value is a literal character. URL encoding applies to the field name and value independently, so any `=` appearing *within* a data value is encoded as `%3D` (e.g., a value of `a=b` becomes `a%3Db`). Simple alphanumeric values and hyphens pass through unencoded.

### How duplication is prevented

Each writer writes to its **own granular lock file**. Writers never overwrite each other's progress. Returning to the earlier example:

- Writer A commits offsets {100, 102, 104} → writes `committedOffset = 104` to `0/date%3D2024-01-01.lock`
- Writer B commits offsets {101, 103} → writes `committedOffset = 103` to `0/date%3D2024-01-02.lock`

On restart, each writer's `shouldSkip` method consults **its own granular lock**, so already-committed records are correctly identified and skipped regardless of commit order. Writer A's progress is never overwritten by Writer B.

### How data loss is prevented

The `preCommit` method now tracks the **first buffered (uncommitted) offset** of each writer. When computing the offset to report to Kafka Connect, it uses:

```
globalSafeOffset = min(firstBufferedOffset across all writers with uncommitted data)
```

This ensures Kafka **never advances past the lowest offset that might not be safely in storage**. Returning to the data loss scenario:

- Writer B committed to 105. Writer A has buffered data starting at offset 100.
- `globalSafeOffset = min(100, 106) = 100`
- Kafka Connect commits consumer offset **100**, not 106.

If the process now crashes, Kafka seeks back to offset 100. All of Writer A's data is re-delivered. Granular locks ensure Writer B's already-committed records (101, 103, 105) are correctly skipped via `shouldSkip`.

When **no** writer has uncommitted data (all writers are in `NoWriter` state after committing), `globalSafeOffset` is defined as `max(committedOffset across all writers) + 1`. This is the happy path: Kafka is told it can advance fully to the highest committed offset. Returning nothing for the partition would prevent Kafka from ever committing, causing unbounded consumer lag on restart. `WriterManager.getOffsetAndMeta` handles this case explicitly -- when `firstBufferedOffsets` is empty, it falls through to `committedOffsets.map(_.value).max + 1`.

### The exactly-once invariant

Two properties combine to provide exactly-once semantics:

1. **No data loss**: `globalSafeOffset ≤ min(firstBufferedOffset)` across all active writers. Kafka never advances past uncommitted data.
2. **No duplication**: Each writer consults its own granular lock for deduplication. Records that were already committed to storage are skipped on replay.

Together, every record is written to cloud storage **exactly once**.

### Master lock semantics

The master lock stores `committedOffset = globalSafeOffset - 1`. This preserves the existing semantic that `committedOffset` means "the highest offset durably committed to storage." On startup, the consumer seeks to this offset, `shouldSkip` returns true for it (since it was already committed), and processing begins at the next offset.

This is important for two reasons:

1. **Backward compatibility**: If old code reads a master lock written by new code, the value has the same semantic the old code expects. No data loss occurs.
2. **Correctness**: The one-record-overlap-then-skip pattern that the existing `CloudSinkTask.open()` relies on is preserved exactly.

---

## Lifecycle: How It All Fits Together

### Single-threaded contract

Kafka Connect calls `put` and `preCommit` on the same thread per task. The correctness of the `globalSafeOffset` computation depends on this guarantee: because there is no concurrent delivery of records during `preCommit`, the snapshot of `firstBufferedOffset` values is stable at the time `preCommit` runs. If this contract were ever violated (e.g. by an async delivery path), the `globalSafeOffset` could be computed over an inconsistent view of writer states, undermining the data-loss prevention invariant. All analysis in this document assumes the single-threaded contract holds.

### Normal operation

1. **Startup** (`open`): The index manager reads only the master lock to get the last known safe offset. Granular locks are **not** read at startup. When the first record for a given partition key arrives and a writer is created, `ensureGranularLock` attempts to read the lock file from cloud storage (via `getBlobAsObject`); if it exists, the result is cached immediately so that the subsequent `getSeekedOffsetForPartitionKey` call is a cache hit rather than a second storage read. If the file does not exist (`FileNotFoundError`), it creates one with `NoOverwriteExistingObject`. The writer then lazily loads its own granular lock from cloud storage on the first call to `getSeekedOffsetForPartitionKey`. If no granular lock file exists yet (e.g. fresh deployment or new partition key), the writer falls back to the master lock offset for its `shouldSkip` decisions.

2. **Writing**: Records arrive from Kafka. Each record is routed to the appropriate writer based on its partition values. The writer tracks its `firstBufferedOffset` (the offset of the first record it received since the last commit) and `uncommittedOffset` (the latest offset it has received).

3. **Commit** (flush): When a writer's commit policy is satisfied (record count, file size, or time interval), it uploads its file to cloud storage and updates its **granular lock** with the new `committedOffset`.

4. **preCommit**: Kafka Connect periodically asks the connector what offsets are safe to commit. The connector computes `globalSafeOffset` as described above, updates the **master lock**, schedules garbage collection of obsolete granular locks (see below), and returns `globalSafeOffset` to Kafka Connect. If the master lock update fails, `preCommit` returns no offset for that partition -- Kafka Connect does not advance the consumer offset, preserving exactly-once semantics. The cached eTag is **not** refreshed on failure. For transient cloud errors (network timeout, throttling), the eTag remains valid and the next `preCommit` cycle will succeed without intervention. For eTag mismatches (another task modified the lock), the stale eTag causes repeated failures on subsequent cycles -- this is the correct fencing behavior, preventing a zombie task from overwriting the new task's master lock and from advancing its consumer offset.

5. **Crash recovery**: On restart, the index manager reads only the master lock. Writers are not recreated upfront -- they are created on demand as records arrive. When a writer is first created for a given partition key, it lazily loads its granular lock from storage. This means granular locks for partition keys that do not receive new records after restart are never read at all, avoiding unnecessary I/O. The consumer resumes from the master lock's offset, and `shouldSkip` uses the lazily-loaded granular lock (or falls back to the master offset) for per-writer deduplication.

### Garbage collection

Over time, granular lock files accumulate. GC is split into two phases: a synchronous **enqueue** phase that runs inline during `preCommit`, and an asynchronous **drain** phase that runs on a background timer.

#### Enqueue phase (synchronous, inside `preCommit`)

After a successful master lock update, `cleanUpObsoleteLocks` identifies obsolete granular locks -- those whose `committedOffset` is below the `globalSafeOffset` and that do not belong to an active writer. For each obsolete entry it:

1. Evicts the entry from the in-memory `granularCache` immediately.
2. Enqueues the cloud storage path of the lock file onto a `ConcurrentLinkedQueue` (`gcQueue`).

This phase performs **no cloud I/O** and returns immediately, so it cannot contribute to `preCommit` latency or cause `offset.flush.timeout.ms` breaches.

Two safety guards apply to the enqueue decision:

1. **Master lock durability gate**: GC only runs after the master lock has been successfully updated. If the master lock update fails, GC is skipped entirely. This prevents the scenario where granular locks are deleted but the master lock still points to an old offset -- on crash recovery, the consumer would seek to the stale master offset but the granular locks needed for deduplication would be gone, causing data duplication.

2. **Active writer exclusion**: Lock files for partition keys that currently have an active writer are never enqueued, even if their committed offset is below the threshold. Active writers need the lock file for their next conditional commit. Deleting an active writer's lock would cause a `FatalCloudSinkError` on the next flush because the eTag required for the conditional write can no longer be resolved.

#### Drain phase (asynchronous, background timer)

A `ScheduledExecutorService` runs a `drainGcQueue` task at a configurable interval (`connect.<prefix>.indexes.gc.interval.seconds`, default 300 seconds). On each tick:

1. The queue is drained into a local buffer.
2. Paths are grouped by bucket.
3. Each bucket's paths are chunked into batches of configurable size (`connect.<prefix>.indexes.gc.batch.size`, default 1000) and deleted via `storageInterface.deleteFiles`. On S3 this maps to the `DeleteObjects` API (up to 1,000 keys per call); on GCS and Azure the storage interface handles batching internally.
4. Delete failures are logged at WARN level but do not propagate -- orphaned lock files are harmless and will be re-enqueued or swept manually. They do not affect correctness.

Partial batches (fewer items than `gcBatchSize`) are deleted normally -- the batch size is an upper bound, not a minimum. Every enqueued path is deleted on the next drain tick regardless of how many items are queued.

#### Task shutdown

When the connector task is stopped (`CloudSinkTask.stop()`), `indexManager.close()` is called. This shuts down the `ScheduledExecutorService` and performs a final synchronous drain of any remaining items in `gcQueue`, ensuring that lock files identified for deletion during the last `preCommit` cycle are cleaned up before the task exits.

#### Scope limitation

`cleanUpObsoleteLocks` only operates on lock entries that are present in the in-memory cache. Granular lock files in cloud storage that were never loaded into the cache (e.g., partition keys from prior runs that no longer receive data) are not cleaned up. Over time with high-cardinality `PARTITIONBY` and evolving partition keys, these orphaned lock files accumulate. These files are small (a few hundred bytes each) and do not affect correctness, but operators running high-cardinality workloads with transient partition keys should periodically sweep the `.locks/<topic>/<partition>/` directories to remove stale files.

### Zombie task and temp-upload fencing

When indexing is enabled, each writer commit follows a three-phase protocol:

1. **Phase 1 -- Upload**: The local staging file is uploaded to a temporary path under `.temp-upload/<topic>/<partition>/<UUID>/...`.
2. **Phase 2 -- Copy**: The file is copied from the temporary path to the final output path.
3. **Phase 3 -- Delete**: The temporary file is deleted.

Each phase is followed by an eTag-conditional update of the writer's lock file (granular or master). The eTag acts as a fencing token: if two tasks overlap (a "zombie" that woke up after a GC pause, and the new task that took over after a rebalance), only one can succeed at the conditional lock update. The other's update fails with an eTag mismatch.

**Critical invariant**: The fencing guarantee depends on eTags being held exclusively in memory and never re-read from shared storage during a commit. If a task discards its eTag (e.g. via LRU eviction) and re-reads the current eTag from storage, it effectively steals the new task's fencing token, defeating the mechanism. For this reason, `updateForPartitionKey` treats a granular lock eTag cache miss as a `FatalCloudSinkError` rather than transparently re-reading from storage. Similarly, `updateMasterLock` does not refresh its cached eTag on write failure.

This prevents data duplication at the final output path in all verified scenarios:

- If the zombie completes Phase 1 (upload to temp) but its Phase 2 lock update fails (eTag mismatch because the new task already modified the lock): the data file sits under `.temp-upload/...` and never reaches the final output path. Downstream query engines reading the output directory do not see it.
- If the zombie completed Phases 1-2 (upload + copy + lock update with PendingState) before pausing: the new task reads the lock on startup, sees the PendingState, and resolves it -- completing the copy and recording the committed offset. No duplication.
- If the zombie completed all three phases before pausing: the new task already resolved the PendingState. The zombie's subsequent lock update fails due to eTag mismatch. No duplication.

**Residual gap -- orphaned temp files**: When a zombie's Phase 2 lock update fails, the uploaded temp file at `.temp-upload/...` is never cleaned up. No PendingState was recorded (the recording attempt failed), so no future task knows about it. These files accumulate silently. Operators should run a periodic sweep to delete `.temp-upload/` files older than a configurable threshold (e.g. 2x the longest expected task runtime).

### Lazy loading and in-memory cache

Granular lock metadata (committed offsets and eTags) is held in a bounded in-memory cache rather than loaded all at startup. This section describes the full lifecycle of that cache.

**Lazy loading on first writer use**

When a `Writer` is created for a new partition key, it calls `getSeekedOffsetForPartitionKey`. On a cache miss, `IndexManagerV2` reads the corresponding granular lock file from cloud storage and populates the cache. If the file does not exist (`FileNotFoundError`), nothing is cached and the writer falls back to the master lock offset. If the file contains a `PendingState` (from a previous crash mid-commit), the pending upload/copy/delete operations are resolved before the offset is cached.

Reads triggered by lazy loading are logged at INFO level so operators can observe which partition keys are being initialised.

**LRU-bounded capacity**

The cache has a configurable maximum entry count (default: `10,000`). It is implemented as a single access-ordered `LinkedHashMap` where each entry holds the committed offset and eTag. When the cache exceeds its capacity, the least-recently-used entry is automatically evicted. All cache operations (get, put, remove, eviction) are O(1). LRU evictions are logged at WARN level; frequent warnings indicate that `maxGranularCacheSize` should be increased for the workload.

If an active writer's cache entry is evicted and the writer subsequently tries to commit, `updateForPartitionKey` detects the missing eTag and raises a `FatalCloudSinkError`. This is intentional: re-reading the eTag from storage would defeat the zombie-task fencing mechanism (see "Zombie task and temp-upload fencing"). The task will restart and re-initialize all locks from storage. **Operators must size `maxGranularCacheSize` to cover the full active working set** to avoid this failure mode. Frequent `FatalCloudSinkError` crashes with messages referencing `maxGranularCacheSize` indicate the value needs to be increased.

**Lifecycle eviction (writer close and partition reassignment)**

To prevent memory leaks, cache entries are explicitly evicted at lifecycle boundaries:

- `Writer.close()`: evicts the single `(topicPartition, partitionKey)` entry for that writer. This happens on graceful shutdown, rebalance-driven close, and schema-change rollover.
- `WriterManager.cleanUp(topicPartition)`: evicts all granular lock entries for the topic-partition being reassigned away.
- `WriterManager.close()`: evicts all granular lock entries for every topic-partition owned by the task (called during connector stop).

Eviction removes entries from the in-memory cache only. The lock files in cloud storage are **not** affected -- they remain until `cleanUpObsoleteLocks` deletes them at the next `preCommit` cycle.

**Re-loading after eviction**

If a new writer is later created for the same partition key -- e.g. after a rebalance hands the partition back to this task -- the granular lock file is re-read from storage via `getSeekedOffsetForPartitionKey` (lazy load on writer creation). This is the **only** code path that re-reads eTags from storage. The commit path (`updateForPartitionKey`) does **not** re-read on cache miss; instead it raises a `FatalCloudSinkError` to preserve the zombie-fencing invariant (see "Zombie task and temp-upload fencing"). This means LRU eviction of an active writer's entry is a fatal condition that requires a task restart, rather than a transparent recovery.

---

## Migration and Compatibility

### Upgrading from the old single-lock system

The upgrade is **fully transparent**:

1. On first startup after upgrade, `open()` reads the existing master lock. Granular locks are not read at startup and none exist yet anyway.
2. When the first records arrive, each writer lazily attempts to load its granular lock from storage, finds none, and falls back to the master lock for `shouldSkip` decisions -- identical to pre-upgrade behavior.
3. On first commit, each writer creates its granular lock file. The two-tier system is now active.
4. On first `preCommit`, `globalSafeOffset` is written to the master lock.

No manual migration, no config changes, no downtime beyond the normal restart for deploying new JARs.

### Rolling upgrades

Kafka Connect assigns each partition to exactly one task at a time. During a rolling upgrade:

- Old tasks ignore granular lock files (they don't know about them).
- New tasks write granular locks alongside the master lock.
- The master lock is updated by both old code (via `update`) and new code (via `updateMasterLock`) with the same semantic.
- After all workers are upgraded, the new code is fully in control.

### Rollback to old code

Granular lock files become orphaned (old code ignores them). They consume negligible storage and can be manually cleaned up. The master lock written by new code uses `committedOffset = globalSafeOffset - 1`, which is semantically identical to what old code writes. Old code reads it and operates correctly.

### First restart after upgrade

Because the old master lock may have an incorrect offset (due to the regression bug), the first restart may replay already-committed records. Since no granular locks exist yet, these records will be re-processed (potential duplicates). This is the **same behavior as the old code** -- the upgrade does not make it worse. After the first successful batch, granular locks prevent this class of duplicate going forward.

---

## Connectors Covered

This architecture lives in `kafka-connect-cloud-common` and applies to all cloud sink connectors:

| Connector | Sink Task Class |
|-----------|----------------|
| Amazon S3 | `S3SinkTask` |
| Google Cloud Storage | `GCPStorageSinkTask` |
| Azure Data Lake Storage | `DatalakeSinkTask` |

All three extend `CloudSinkTask` and are wired through `WriterManagerCreator.from`, so the exactly-once guarantees propagate automatically with zero connector-specific code.

---

## Operational Constraints and Limits

This section documents known non-functional constraints that operators should be aware of when running with `PARTITIONBY` under high cardinality.

### Unbounded active Writer instances (Critical)

`WriterManager.writers` is an unbounded `mutable.Map`. Each active `Writer` holds a `FormatWriter` (typically 1-10 MB of heap for Parquet/Avro page buffers and dictionary encoders), an open `java.io.File` handle (one file descriptor), and commit-state metadata. Under high-cardinality `PARTITIONBY` (e.g. partitioning by `user_id` or `session_id` with millions of unique values), the connector creates a `Writer` per unique value. At 5 MB overhead per Parquet writer, 10,000 writers consume ~50 GB of heap. The task will OOM or exhaust file descriptors (`EMFILE`, default `ulimit -n` is 1024) long before reaching the LRU lock cache limit.

**Mitigation (not yet implemented)**: An idle-writer reaper that flushes and closes `Writer` instances that have not received data within a configurable interval. The granular lock remains in cloud storage for dedup on future lazy-load.

### GC timing in preCommit (High -- mitigated)

`cleanUpObsoleteLocks` runs inline during `preCommit` (only after a successful master lock update), but it now performs **no cloud I/O**. It evicts obsolete entries from the in-memory cache and enqueues their storage paths onto a `ConcurrentLinkedQueue`. Actual cloud deletes are performed asynchronously by a background `ScheduledExecutorService` that drains the queue at a configurable interval (`connect.<prefix>.indexes.gc.interval.seconds`, default 300s) and issues batched deletes (`connect.<prefix>.indexes.gc.batch.size`, default 1000 keys per call). This decouples GC latency from `preCommit` entirely, eliminating the risk of `offset.flush.timeout.ms` breaches under high cardinality. See "Garbage collection" under "Lifecycle" for full details.

### LRU cache thrashing under high cardinality (High)

When the number of active partition keys exceeds `maxGranularCacheSize` (default 10,000), the LRU cache evicts entries that may belong to active writers. If an active writer's eTag is evicted, its next commit raises a `FatalCloudSinkError` and the task restarts. This is necessary to preserve zombie-task fencing: transparently re-reading eTags from storage on cache miss would allow a zombie task to steal a new task's fencing token, leading to data duplication (see "Zombie task and temp-upload fencing").

**Mitigation**: `maxGranularCacheSize` **must** be sized to cover the full active working set (the number of distinct PARTITIONBY values with active writers across all assigned topic-partitions). A future improvement could pin cache entries for writers that are still alive in `WriterManager.writers`, ensuring LRU eviction only targets entries for closed/cleaned-up writers.

### Lazy-load burst at startup (Medium)

After a crash with a large replay window and high cardinality, the first `put` call triggers a burst of synchronous cloud GETs -- one for each unique partition key in the replayed batch. This can cause the first `put` to take minutes, potentially triggering Kafka Connect timeouts.

**Mitigation (not yet implemented)**: Batch lazy-load reads using `parTraverse` with configurable parallelism to avoid overwhelming the cloud API, similar to how `open` already parallelizes master lock reads.

### Orphaned temp files (Medium)

When a zombie task's Phase 2 lock update fails after uploading to `.temp-upload/...`, the temp file is never cleaned up and no `PendingState` records its existence. These files accumulate silently. See "Zombie task and temp-upload fencing" for details.

**Mitigation**: Operators should run a periodic sweep to delete `.temp-upload/` files older than a configurable threshold (e.g. 2x the longest expected task runtime). For stronger guarantees, consider integration with a table format (Iceberg/Delta/Hudi) where file registration is atomic with the commit.

### Thundering herd commit (Medium)

`WriterCommitManager.commitWritersWithFilter` commits **all** writers for a `TopicPartition` when any one writer triggers a flush. Under high cardinality, one timer tick can trigger thousands of tiny commits (each writing a granular lock file + uploading a data file). This is correct for the `globalSafeOffset` computation -- bulk commit allows the offset to advance maximally -- but creates a thundering herd of cloud API calls and increases the blast radius of transient errors.

**Mitigation**: Operators should size `offset.flush.interval.ms` and commit thresholds (record count, file size) conservatively for high-cardinality workloads to avoid triggering bulk commits with many small files.

---

## Crash Safety Guarantees

| Scenario | Outcome |
|----------|---------|
| Crash after writer commit, before `preCommit` | Granular lock is up to date. Master lock may be stale. On restart, consumer re-reads from master lock offset. Granular locks deduplicate. **No data loss, possible replay (no duplication due to granular locks).** |
| Crash during `preCommit` (master lock update) | eTag-based atomic write means the master lock either updated or didn't. On restart, the last successful state is read. **No corruption.** |
| Crash during garbage collection | Items already enqueued in `gcQueue` but not yet drained are lost. Items already drained but whose batched delete was in flight may be partially deleted. In both cases the orphaned granular lock files are harmless -- they sit below the `globalSafeOffset` watermark and are either re-enqueued on the next run or swept manually. **No impact on correctness.** |
| Crash with `PendingState` on a granular lock | `PendingState` is detected and resolved during **lazy load**, when the writer first calls `getSeekedOffsetForPartitionKey` for that partition key. If the local staging file still exists, the pending upload is completed. If the staging file is gone (the common case after a crash), the pending operation is abandoned (`cancelPending=true`) and the records revert to "uncommitted". On replay from the master lock offset, these records are re-delivered and re-processed. The write is **re-done from scratch**, not resumed. **No data loss, no duplication.** |
| Crash with `PendingState` on master lock | Existing behavior, unchanged. On restart, pending operations are completed. **No data loss.** |
| Zombie task uploads to `.temp-upload/` but Phase 2 eTag update fails | Temp file is orphaned at `.temp-upload/...`. Final output path is unaffected. **No duplication.** Storage leak requires periodic sweep (see "Zombie task and temp-upload fencing"). |
| Master lock update fails repeatedly at `preCommit` | `preCommit` returns no offset for the affected partition -- Kafka Connect does not advance the consumer offset. Master lock remains stale. GC is skipped (it only runs after a successful master lock update), so all granular locks are preserved. On crash, consumer replays from the stale offset. Granular locks deduplicate already-committed records. **No data loss, no duplication.** The eTag is not refreshed on failure -- for transient errors it remains valid and the next cycle succeeds; for eTag mismatches (fencing) repeated failure is the correct behavior, preventing a zombie task from advancing. |

---

## Summary

| Concern | Before (single lock) | After (two-tier locks) |
|---------|---------------------|----------------------|
| **Duplication** | Writers overwrite each other's committed offset → offset regression → replayed records are re-written | Each writer has its own granular lock → no interference → `shouldSkip` is per-writer accurate |
| **Data loss** | `preCommit` returns max committed offset → Kafka advances past uncommitted buffered data → crash loses records | `preCommit` returns `min(firstBufferedOffset)` → Kafka never advances past uncommitted data |
| **Exactly-once** | Not guaranteed with PARTITIONBY | Guaranteed: no data loss (globalSafeOffset invariant) + no duplication (granular lock dedup) |
| **Migration** | N/A | Fully transparent, zero-downtime upgrade |
| **Rollback** | N/A | Safe: master lock is backward-compatible, granular locks are ignored by old code |
