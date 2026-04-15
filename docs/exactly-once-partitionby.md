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

The structural `=` between field name and value is a literal character. URL encoding applies to the field name and value independently, so any `=` appearing *within* a data value is encoded as `%3D` (e.g., a value of `a=b` becomes `a%3Db`). Underscores are encoded as `%5F` because `_` is used as the separator between field-value pairs — without this encoding, distinct partition value combinations containing underscores could produce identical keys (e.g., fields `{"a" → "x_y", "z" → "1"}` and `{"a" → "x", "y_z" → "1"}` would both produce `a=x_y_z=1`). Simple alphanumeric values and hyphens (excluding underscores) pass through unencoded.

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

### Monotonicity invariant on globalSafeOffset

`globalSafeOffset` must be **monotonically non-decreasing** within the lifetime of a task instance. If it were allowed to regress (e.g. because an idle writer with a high committed offset was evicted from memory), the master lock and consumer offset would move backwards, and on crash recovery records that were already committed would be replayed. Combined with granular lock GC -- which deletes lock files below `globalSafeOffset` -- the deduplication data for those records may no longer exist, causing data duplication.

`WriterManager` enforces monotonicity by maintaining a per-partition high-watermark (`safeOffsetHighWatermarks`). On each `preCommit` call the calculated offset is floored at the previous high-watermark:

```
globalSafeOffset = max(calculatedSafeOffset, previousHighWatermark)
```

The high-watermark is initialized on first `preCommit` from the master lock offset read during `open()` (i.e., `committedOffset + 1`). This prevents the persistent master lock from regressing when a restarted task receives records for different partition keys than the previous instance (e.g. date-based partitioning where old dates stop appearing).

The high-watermark is cleared when the partition is removed via `cleanUp` (rebalance) or `close` (task shutdown), because on re-assignment the master lock in cloud storage is the authoritative starting point. After re-assignment, the high-watermark is re-initialized from the freshly read master lock offset.

### The exactly-once invariant

Three properties combine to provide exactly-once semantics:

1. **No data loss**: `globalSafeOffset ≤ min(firstBufferedOffset)` across all active writers. Kafka never advances past uncommitted data.
2. **No duplication**: Each writer consults its own granular lock for deduplication. Records that were already committed to storage are skipped on replay.
3. **Monotonicity**: `globalSafeOffset` never regresses within a task instance. The master lock and consumer offset can only advance, ensuring that GC decisions remain valid and crash recovery never replays a window whose deduplication state was already cleaned up.

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

1. **Startup** (`open`): The index manager reads only the master lock to get the last known safe offset. Granular locks are **not** read at startup -- they are lazily loaded when the first record for a given partition key arrives (see "Lazy loading and in-memory cache" for the full cache lifecycle). If no granular lock file exists yet (e.g. fresh deployment or new partition key), the writer falls back to the master lock offset for its `shouldSkip` decisions.

2. **Writing**: Records arrive from Kafka. Each record is routed to the appropriate writer based on its partition values. The writer tracks its `firstBufferedOffset` (the offset of the first record it received since the last commit) and `uncommittedOffset` (the latest offset it has received).

3. **Commit** (flush): When a writer's commit policy is satisfied (record count, file size, or time interval), it uploads its file to cloud storage and updates its **granular lock** with the new `committedOffset`.

4. **preCommit**: Kafka Connect periodically asks the connector what offsets are safe to commit. The connector computes `globalSafeOffset` as described above and returns it to Kafka Connect. In **PARTITIONBY mode**, `preCommit` writes the master lock via `updateMasterLock`, schedules garbage collection of obsolete granular locks (see below), and returns `globalSafeOffset`. If the master lock update fails, `preCommit` returns no offset for that partition -- Kafka Connect does not advance the consumer offset, preserving exactly-once semantics. The cached eTag is **not** refreshed on failure. For transient cloud errors (network timeout, throttling), the eTag remains valid and the next `preCommit` cycle will succeed without intervention. For eTag mismatches (another task modified the lock), the stale eTag causes repeated failures on subsequent cycles -- this is the correct fencing behavior, preventing a zombie task from overwriting the new task's master lock and from advancing its consumer offset. In **non-PARTITIONBY mode**, `preCommit` does **not** call `updateMasterLock` because `Writer.commit()` already maintains the master lock via `indexManager.update()` -- the two methods write to the same lock file path with the same committed offset, so the `preCommit` write would be redundant. Skipping it halves the master lock write frequency and avoids one eTag-conditional cloud API call per `preCommit` cycle. The local high-watermark (`safeOffsetHighWatermarks`) is still updated to preserve the monotonicity invariant.

5. **Crash recovery**: On restart, the index manager reads only the master lock. Writers are not recreated upfront -- they are created on demand as records arrive. When a writer is first created for a given partition key, `ensureGranularLock` reads the granular lock from storage. If the lock contains a `PendingState` (from a crash mid-commit), the pending upload/copy/delete operations are resolved before the writer proceeds. This means granular locks for partition keys that do not receive new records after restart are never read at all, avoiding unnecessary I/O. The consumer resumes from the master lock's offset, and `shouldSkip` uses the lazily-loaded granular lock (or falls back to the master offset) for per-writer deduplication.

### Garbage collection

Over time, granular lock files accumulate. GC is split into two phases: a synchronous **enqueue** phase that runs inline during `preCommit`, and an asynchronous **drain** phase that runs on a background timer.

#### Enqueue phase (synchronous, inside `preCommit`)

After a successful master lock update, `cleanUpObsoleteLocks` identifies obsolete granular locks -- those whose `committedOffset` is below the `globalSafeOffset` and that do not belong to an active writer. For each obsolete entry it:

1. Evicts the entry from the in-memory `granularCache` immediately.
2. Enqueues the cloud storage path of the lock file onto a `ConcurrentLinkedQueue` (`gcQueue`).

This phase performs **no cloud I/O** and returns immediately, so it cannot contribute to `preCommit` latency or cause `offset.flush.timeout.ms` breaches.

Two safety guards apply to the enqueue decision:

1. **Master lock durability gate**: GC only runs after the master lock has been successfully updated. If the master lock update fails, GC is skipped entirely. This prevents the scenario where granular locks are deleted but the master lock still points to an old offset -- on crash recovery, the consumer would seek to the stale master offset but the granular locks needed for deduplication would be gone, causing data duplication.

2. **Writer map exclusion**: Lock files for partition keys that currently have a writer in the `writers` map are never enqueued, regardless of the writer's state. Idle writers are eagerly evicted whenever a new writer is created (see "Idle writer eviction"), and the cache entry is evicted at the same time. Once evicted, the lock file in cloud storage becomes an orphan for the periodic sweep, or is re-read from storage if a new record for that partition key triggers `createWriter` → `ensureGranularLock`.

#### Drain phase (asynchronous, background timer)

A `ScheduledExecutorService` runs a `drainGcQueue` task at a configurable interval (`connect.<prefix>.indexes.gc.interval.seconds`, default 300 seconds). On each tick:

1. The queue is drained into a local buffer. For each dequeued item, the drain checks whether the partition key has been **reclaimed** by a new writer (i.e., `granularCache.containsKey(tp, pk)` returns `true`). If reclaimed, the item is silently skipped -- the file is needed by the new writer and must not be deleted. This check eliminates the race condition where `cleanUpObsoleteLocks` enqueues a lock for deletion, but a new writer for the same partition key is created before the drain runs.
2. Non-reclaimed paths are grouped by bucket.
3. Each bucket's paths are chunked into batches of configurable size (`connect.<prefix>.indexes.gc.batch.size`, default 1000) and deleted via `storageInterface.deleteFiles`. On S3 this maps to the `DeleteObjects` API (up to 1,000 keys per call); on GCS and Azure the storage interface handles batching internally.
4. Delete failures are logged at WARN level but do not propagate -- orphaned lock files are harmless and will be re-enqueued on the next run or cleaned up by the periodic orphan sweep (see "Orphaned lock sweep" below). They do not affect correctness.

Partial batches (fewer items than `gcBatchSize`) are deleted normally -- the batch size is an upper bound, not a minimum. Every non-reclaimed enqueued path is deleted on the next drain tick regardless of how many items are queued.

#### Task shutdown

Kafka Connect calls `close(allPartitions)` before `stop()`. `CloudSinkTask.close()` calls `WriterManager.close()`, which closes all writers and evicts granular lock cache entries via `evictAllGranularLocks`, but deliberately does **not** call `clearTopicPartitionState`. `CloudSinkTask.close()` itself also does **not** call `clearTopicPartitionState`. The `seekedOffsets` map must remain populated so that the subsequent `indexManager.close()` -- called from `CloudSinkTask.stop()`, which shuts down the `ScheduledExecutorService` and performs a final synchronous drain of any remaining items in `gcQueue` -- can distinguish items that belong to this task from items for revoked partitions. If `seekedOffsets` were cleared before the drain, every queued GC item would be discarded as "partition no longer owned," and the final-drain cleanup would never delete anything. The `indexManager` reference (along with its internal state) is set to `null` immediately after `close()` returns and is garbage-collected.

#### Rebalance (partition reassignment)

During a rebalance, Kafka Connect calls `close(currentPartitions)` followed by `open(newPartitions)` -- `stop()` is **not** called. Because `WriterManager.close()` deliberately does not call `clearTopicPartitionState`, `seekedOffsets` entries for revoked partitions would otherwise survive indefinitely. The background `sweepOrphanedLocks` iterates `seekedOffsets.keys` and would process revoked partitions, issuing LIST and GET API calls on partitions now owned by another task. Similarly, `drainGcQueue` would treat items for revoked partitions as owned (since `seekedOffsets.contains` returns true).

To prevent this, `IndexManagerV2.open()` prunes stale state before processing the new partition set. It computes `stalePartitions = seekedOffsets.keys -- newTopicPartitions` and calls `evictAllGranularLocks` and `clearTopicPartitionState` for each stale partition. This is safe because:

- During shutdown, `open()` is never called between `close()` and `stop()`, so the shutdown path is unaffected -- `seekedOffsets` remains populated for the final GC drain.
- On the first `open()` call, `seekedOffsets` is empty and no pruning occurs.
- On rebalance, exactly the revoked partitions are cleaned up, while retained and newly assigned partitions proceed through the normal `open()` flow.

**Best-effort background-thread gate**: Between `close()` returning and `open()` completing the prune, there is a brief window where background threads could observe stale `seekedOffsets`. To minimize this, `CloudSinkTask.close()` calls `indexManager.suspendBackgroundWork()` before `writerManager.close()`, which sets a `@volatile var acceptingWork = false` flag on `IndexManagerV2`. The scheduled executor lambdas check this flag before invoking `drainGcQueue` or `sweepOrphanedLocks` -- if false, the invocation is skipped. `open()` sets the flag back to `true` after pruning stale partitions and processing the new assignment.

This is a **best-effort gate, not mutual exclusion**: the volatile flag cannot stop a background thread that has already entered `drainGcQueue()` or `sweepOrphanedLocks()` at the instant the flag is flipped. In-flight executions are benign:

- **`drainGcQueue()`**: Processes items that were enqueued before the rebalance by `cleanUpObsoleteLocks`. Those items were correctly identified as obsolete (committed offset below `globalSafeOffset`, no active writer). Deleting them is safe regardless of which task now owns the partition -- the data they tracked is already durably committed, and if the new owner later needs a lock, `ensureGranularLock` recreates it with a fall-back to the master lock offset.
- **`sweepOrphanedLocks()`**: Issues read-only LIST and GET API calls on revoked partitions (wasteful but harmless -- no writes, no corruption). Any `GcItem`s it enqueues for those partitions will be discarded by the *next* `drainGcQueue()` run, because by then `open()` will have pruned `seekedOffsets` and the drain's `seekedOffsets.contains` check will return false.

On the **shutdown path** (`close()` → `stop()`), the flag stays false. The final synchronous `drainGcQueue()` in `IndexManagerV2.close()` is called directly (not through the scheduled lambda), so it bypasses the gate and runs regardless of the flag's value.

#### Orphaned lock sweep

`cleanUpObsoleteLocks` only operates on lock entries present in the in-memory `granularCache`. Granular lock files from prior runs that no longer receive data are never loaded into the cache and therefore never cleaned up by the regular GC. Over time with high-cardinality `PARTITIONBY` and evolving partition keys, these orphaned lock files accumulate in cloud storage.

To address this, a **periodic orphan sweep** runs on a separate `ScheduledExecutorService` (`sweepExecutor`), decoupled from the regular GC drain timer. Each task sweeps only its own partitions. The sweep is a **discovery-only** phase -- it lists lock files in cloud storage, filters them, and enqueues `GcItem`s into the existing `gcQueue`. Actual deletion is handled by `drainGcQueue` on the existing `gcExecutor`, which applies the same cache-gated reclaim check as regular GC items.

**Scheduling**: The sweep executor fires at the configured `gcSweepIntervalSeconds` interval (default 86400s = 24h). A **persistent sweep marker file** (`sweep-marker.json`) is stored per topic-partition, under the same `.locks/<topic>/<partition>/` directory as the granular locks. The marker stores `lastRunEpochMillis` and `nextRunEpochMillis`. Each partition is swept independently: before scanning a partition, the sweep reads the existing marker (capturing its eTag) and then writes a new marker using an **eTag-conditional write** (write-before-sweep pattern). When the marker file does not yet exist, a `NoOverwriteExistingObject` precondition (`ifNoneMatch("*")`) is used; when the marker exists but has expired, an `ObjectWithETag` precondition (`ifMatch(eTag)`) is used. This makes the read-then-write an atomic check-and-set: if two tasks temporarily believe they own the same partition after a rebalance, only one task's conditional write succeeds -- the other receives an eTag mismatch error and skips the sweep for that partition. A partition whose marker has not yet expired is skipped without attempting a write. On rebalance, the marker travels with the partition -- the new task reads the existing marker and respects its schedule, preventing premature re-sweeps. This ensures that a crash mid-sweep does not trigger an immediate re-sweep on restart -- the markers persist across restarts and rebalances.

**Filtering**: For each owned `TopicPartition`, the sweep calls `listFileMetaRecursive` to enumerate lock files, then applies:

1. **Recency filter**: Files with `lastModified` newer than `gcSweepMinAgeSeconds` are skipped without a GET read (configured via `connect.<prefix>.indexes.gc.sweep.min.age.seconds`, default 86400 = 24h).
2. **Cache exclusion**: Files whose partition key is already in `granularCache` are skipped (already tracked by regular GC).
3. **GET cap**: A global budget of `gcSweepMaxReads` GET requests per sweep cycle across all partitions (configured via `connect.<prefix>.indexes.gc.sweep.max.reads`, default 1000). When exhausted, remaining partitions are deferred to the next cycle.
4. **Offset comparison**: The lock file is read and its `committedOffset` is compared against the master lock offset (`seekedOffsets.get(tp)`). Only lock files with `committedOffset <= masterLockOffset` are enqueued as orphans. Since the master lock stores `globalSafeOffset - 1`, this is equivalent to `committedOffset < globalSafeOffset`, matching the threshold used by `cleanUpObsoleteLocks`.

Lock files with no `committedOffset` (freshly created) are skipped. Partitions without a master lock offset in `seekedOffsets` (e.g. brand-new partitions that have never had a commit) are also skipped -- there is no safe baseline to compare against.

**Configuration**:

- `connect.<prefix>.indexes.gc.sweep.enabled` -- enable or disable the orphan sweep (default `true`).
- `connect.<prefix>.indexes.gc.sweep.interval.seconds` -- interval between sweep runs (default 86400 = 24h).
- `connect.<prefix>.indexes.gc.sweep.min.age.seconds` -- minimum age (in seconds) a lock file must have before the sweep considers it for deletion (default 86400 = 24h).
- `connect.<prefix>.indexes.gc.sweep.max.reads` -- per-cycle GET cap across all partitions (default 1000).

**Exactly-once safety**: The sweep preserves exactly-once guarantees because (a) only lock files with `committedOffset` at or below the master lock's `committedOffset` (equivalently, below `globalSafeOffset`) are enqueued, meaning their data has already been committed; (b) `drainGcQueue` checks `granularCache.containsKey` before deleting, protecting any file reclaimed by a new writer; (c) if a deleted lock is later needed by a new writer, `ensureGranularLock` recreates it and the writer falls back to the master lock offset for deduplication; and (d) the sweep never writes to lock files, so writers' eTags are unaffected.

### Zombie task and temp-upload fencing

When indexing is enabled, each writer commit follows a three-phase protocol:

1. **Phase 1 -- Upload**: The local staging file is uploaded to a temporary path under `.temp-upload/<topic>/<partition>/<UUID>/...`.
2. **Phase 2 -- Copy**: The file is copied from the temporary path to the final output path.
3. **Phase 3 -- Delete**: The temporary file is deleted.

Each phase is followed by an eTag-conditional update of the writer's lock file (granular or master). The eTag acts as a fencing token: if two tasks overlap (a "zombie" that woke up after a GC pause, and the new task that took over after a rebalance), only one can succeed at the conditional lock update. The other's update fails with an eTag mismatch.

**Critical invariant**: The fencing guarantee depends on eTags being held exclusively in memory and never re-read from shared storage during a commit. If a task discards its eTag and re-reads the current eTag from storage, it effectively steals the new task's fencing token, defeating the mechanism. For this reason, `updateForPartitionKey` treats a granular lock eTag cache miss as a `FatalCloudSinkError` rather than transparently re-reading from storage. Similarly, `updateMasterLock` does not refresh its cached eTag on write failure. The granular cache uses no automatic eviction (see "Lazy loading and in-memory cache"), so the only way an active writer's eTag can be missing is through explicit lifecycle eviction (writer close, partition reassignment, or idle-writer eviction) -- all of which invalidate the writer, not just the cache entry.

This prevents data duplication at the final output path in all verified scenarios:

- If the zombie completes Phase 1 (upload to temp) but its Phase 2 lock update fails (eTag mismatch because the new task already modified the lock): the data file sits under `.temp-upload/...` and never reaches the final output path. Downstream query engines reading the output directory do not see it.
- If the zombie completed Phases 1-2 (upload + copy + lock update with PendingState) before pausing: the new task reads the lock on startup, sees the PendingState, and resolves it -- completing the copy and recording the committed offset. No duplication.
- If the zombie completed all three phases before pausing: the new task already resolved the PendingState. The zombie's subsequent lock update fails due to eTag mismatch. No duplication.

**Residual gap -- orphaned temp files**: When a zombie's Phase 2 lock update fails, the uploaded temp file at `.temp-upload/...` is never cleaned up. No PendingState was recorded (the recording attempt failed), so no future task knows about it. These files accumulate silently. Operators should run a periodic sweep to delete `.temp-upload/` files older than a configurable threshold (e.g. 2x the longest expected task runtime).

### Lazy loading and in-memory cache

Granular lock metadata (committed offsets and eTags) is held in an unbounded in-memory cache whose lifecycle is driven entirely by `Writer` objects. This section describes the full lifecycle of that cache.

**Lazy loading on first writer use**

When a `Writer` is created for a new partition key, `WriterManager.createWriter` calls `ensureGranularLock` followed by `getSeekedOffsetForPartitionKey`. Both code paths handle `PendingState` identically: if the granular lock file contains a `PendingState` (from a previous crash mid-commit), the pending upload/copy/delete operations are resolved via `processPendingOperations` before the resolved offset and eTag are cached. `ensureGranularLock` reads the lock file via `tryOpen` and delegates to `resolveAndCacheGranularLock`, which pattern-matches on the `PendingState` field. If the file does not exist (`FileNotFoundError`), `ensureGranularLock` creates an empty lock file. On a cache miss, `getSeekedOffsetForPartitionKey` falls through to `loadGranularLock`, which performs the same `PendingState` resolution. In practice, `ensureGranularLock` populates the cache on its first read, so `getSeekedOffsetForPartitionKey` is a cache hit and `loadGranularLock` is not invoked.

Reads triggered by lazy loading are logged at INFO level so operators can observe which partition keys are being initialised.

**Why PendingState must be resolved before caching**

A `PendingState` in a granular lock file means the previous task instance crashed mid-commit -- between phases of the three-phase Upload → Copy → Delete protocol (see "Zombie task and temp-upload fencing"). The lock file records which operations were still in flight at the time of the crash. When `ensureGranularLock`, `resolveAndCacheGranularLock`, or `loadGranularLock` encounters a `PendingState`, it must resolve the pending operations (complete them or roll them back) **before** caching the offset and eTag. Resolution cannot be deferred for two reasons:

1. **Offset indeterminacy.** The `committedOffset` stored alongside a `PendingState` reflects the state *before* the interrupted commit, not the final truth. If the pending operations complete successfully (e.g. the staged file still exists and is copied to the final path), the true committed offset advances to `pendingOffset`. If they are cancelled (e.g. the staging file no longer exists after the crash), the offset stays at `committedOffset`. The writer's `shouldSkip` logic needs the *resolved* offset to correctly deduplicate records on replay. Caching the pre-resolution value would cause duplication (offset too low → already-committed records are not skipped and are re-written) or data loss (offset too high after a stale cache hit → uncommitted records are skipped because the writer believes they were already committed).

2. **eTag staleness.** `processPendingOperations` writes an updated lock file at each step -- recording the remaining operations list after each completed phase, or clearing the `PendingState` entirely once all operations are done or cancelled. Each of these writes advances the eTag in cloud storage. If the pre-resolution eTag were cached and a writer proceeded to its next commit, the conditional write would fail with an eTag mismatch because the stored eTag no longer matches the lock file. Worse, deferring resolution to a different code path while the writer holds the stale eTag would silently break the zombie-fencing invariant: the writer could never successfully update its lock file, and re-reading the eTag from storage is explicitly forbidden to prevent zombie tasks from stealing fencing tokens (see "Zombie task and temp-upload fencing").

On resolution failure, the cache entry is removed (`granularCache.remove`) so that a subsequent access retries the load-and-resolve sequence cleanly from storage rather than operating on a partially resolved state.

**Unbounded cache, bounded by writer lifecycle**

The granular lock cache is an unbounded `java.util.concurrent.ConcurrentHashMap`. It does **not** perform automatic LRU eviction. Instead, entries are added when writers are created and removed by `cleanUpObsoleteLocks` (GC enqueue phase), `evictAllGranularLocks` (shutdown/rebalance), or explicit `evictGranularLock` calls. `ConcurrentHashMap` is required because the background GC thread reads the cache (`containsKey`) to check whether a scheduled-for-deletion key has been reclaimed by a new writer (see "Drain phase" under "Garbage collection"). All mutating access occurs on the single Kafka Connect task thread.

This design eliminates a previous failure mode: with a bounded `LinkedHashMap` and automatic `removeEldestEntry` eviction, the cache could silently evict an entry for an active writer that still needed its eTag for the next conditional commit. On next flush, `updateForPartitionKey` would detect the missing eTag and raise a `FatalCloudSinkError` -- crashing a healthy task simply because the active working set temporarily exceeded the cache cap. Removing automatic eviction means the cache grows without bound when all writers are actively writing, which is the correct behaviour: every active writer needs its eTag in memory.

In practice, eager eviction on idle writer removal means the steady-state cache size is bounded by the number of concurrently active writers (those in `Writing` or `Uploading` state), not by the total number of unique partition keys ever observed.

If a writer's cache entry is missing at commit time (which can only happen due to a bug or a zombie task whose cache was explicitly evicted), `updateForPartitionKey` raises a `FatalCloudSinkError`. This is intentional: re-reading the eTag from storage would defeat the zombie-task fencing mechanism (see "Zombie task and temp-upload fencing").

**Writer-lifecycle eviction**

Cache entries are explicitly evicted at these lifecycle boundaries:

- `Writer.close()`: does **not** evict the granular cache entry. The entry is deliberately left in the cache so that `cleanUpObsoleteLocks` can detect it as obsolete on the next `preCommit` cycle. If a new writer is created for the same partition key before `preCommit`, `ensureGranularLock` finds a cache hit and reuses the valid eTag without a storage read.
- **Idle writer eviction** (`evictIdleWritersIfNeeded`): evicts the granular cache entry immediately when the writer is removed from the `writers` map. This bounds the cache size to O(active writers). The lock file in cloud storage is untouched -- it becomes an orphan for the periodic sweep or is re-read from storage if a new writer is later created for the same key via `ensureGranularLock`.
- `WriterManager.cleanUp(topicPartition)`: evicts all granular lock entries for the topic-partition being reassigned away (via `evictAllGranularLocks`).
- `WriterManager.close()`: evicts all granular lock entries for every topic-partition owned by the task (via `evictAllGranularLocks`, called during connector stop).
- `cleanUpObsoleteLocks`: removes entries from the cache when enqueuing them for GC (during `preCommit`).

Lock files in cloud storage are **not** affected by cache eviction -- they remain until `cleanUpObsoleteLocks` enqueues them for deletion and the background GC drain deletes them.

**Re-loading after eviction**

If a new writer is later created for the same partition key -- e.g. after idle-writer eviction or after a rebalance hands the partition back to this task -- `ensureGranularLock` checks the cache first. After idle-writer eviction the cache entry has been evicted, so this is always a cache miss: `ensureGranularLock` re-reads the lock file from storage via `tryOpen`, resolves any `PendingState`, and re-populates the cache. If `ensureGranularLock` finds a cache miss, `getSeekedOffsetForPartitionKey` would also re-read via `loadGranularLock`, but in practice the preceding `ensureGranularLock` call always populates the cache first. These are the **only** code paths that read eTags from storage. The commit path (`updateForPartitionKey`) does **not** re-read on cache miss; instead it raises a `FatalCloudSinkError` to preserve the zombie-fencing invariant (see "Zombie task and temp-upload fencing").

### Idle writer eviction

Without eviction, the `writers` map in `WriterManager` grows without bound. Writers in `NoWriter` state (committed, no buffered data) are never removed unless the task shuts down or a rebalance occurs. With high-cardinality `PARTITIONBY` (e.g. 5-minute time buckets over 1000 Kafka partitions), the map would accumulate ~105k entries per partition per year, leading to unbounded heap growth, `O(total writers)` scan costs at every `preCommit`, and neutered GC (idle writers would still count as "active", preventing their lock files from being cleaned up).

**How it works**

Whenever a new writer is created, `WriterManager` eagerly evicts **all** idle writers (those in `NoWriter` state) from the map. This keeps the map lean -- only active writers (buffering or uploading data) and the just-created writer remain. Creating a writer is cheap (typically a granular cache hit, or one cloud GET on a miss), and closing an idle writer is essentially a no-op (it holds no buffered data or open file handles).

The eviction sweep:

1. Iterates the writers map looking for idle writers (`NoWriter` state), excluding the just-created writer.
2. Each idle writer's `close()` is called.
3. The writer is removed from the map.
4. The granular cache entry is evicted immediately via `evictGranularLock`. The lock file in cloud storage is untouched -- it becomes an orphan for the periodic sweep (or is re-read from storage if a new writer is later created for the same key).

Only `NoWriter`-state writers are evictable. Writers in `Writing` or `Uploading` state hold buffered or in-flight data and are never evicted. The just-created writer is excluded from eviction candidates because it starts in `NoWriter` state and would otherwise be immediately evicted.

**Interaction with GC**

`activePartitionKeys` -- the set of partition keys protected from GC -- includes **all** writers currently in the `writers` map, regardless of state. Because idle writers are eagerly evicted, their partition keys leave `activePartitionKeys` promptly, making their lock files eligible for GC on the very next `preCommit` cycle.

Once a writer is evicted from the map, its `close()` is called and the granular cache entry is evicted immediately. The lock file in cloud storage is **not** deleted at this point -- it becomes an orphan. The periodic orphan sweep (see "Orphaned lock sweep") discovers and deletes these files on its next cycle. If a new writer is created for the same partition key before the sweep runs, `ensureGranularLock` re-reads the lock from storage and re-populates the cache, so the file is preserved.

**Interaction with exactly-once guarantees**

Idle writer eviction does not compromise exactly-once semantics, provided the `globalSafeOffset` monotonicity invariant holds:

- **No data loss**: Evicted writers are in `NoWriter` state -- they have no buffered data. The master lock accurately reflects all committed data.
- **No duplication**: When active writers still hold buffered data, `calculatedSafeOffset` equals `min(firstBufferedOffset)` across those writers. Because only `NoWriter`-state writers are evictable, eviction cannot change `firstBufferedOffset` values; the safe offset depends solely on active, buffered writers and is unaffected by eviction. When all writers are idle (no buffered data), `calculatedSafeOffset` falls back to `max(committedOffsets) + 1`. Evicting writers with high committed offsets would cause this value to drop, but the `safeOffsetHighWatermarks` map in `WriterManager` prevents regression: the reported offset is always `max(calculated, previousHighWatermark)`, so the master lock never moves backwards. When a new writer is created for a previously evicted partition key, it reads the granular lock from storage (if it still exists) or falls back to the non-regressed master lock offset. In either case, `shouldSkip` correctly deduplicates already-committed records.
- **Fencing preserved**: The evicted writer is in `NoWriter` state and will never call `updateForPartitionKey`, so the eTag in the evicted cache entry is unreachable -- evicting it has no effect on fencing. If a zombie task tries to commit using a stale eTag, the cloud storage conditional write fails and `updateForPartitionKey` raises a `FatalCloudSinkError` as intended. The zombie-fencing invariant depends on eTags held by *active* writers, not evicted idle ones.

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

### Indexes directory must be unique per connector (Critical)

Lock file paths are derived from `<indexes-dir>/<connector-name>/.locks/<topic>/<partition>/`. If two connectors read the same topic names (even from different Kafka clusters) and write to the same bucket/prefix, they **must** use different indexes directories (`connect.<prefix>.indexes.name`). Otherwise their lock files collide: each connector overwrites the other's master and granular locks, leading to offset regression, duplicate data, and `FatalCloudSinkError` crashes due to eTag mismatches.

This constraint also applies when the same connector name is reused across environments (e.g. staging and production writing to the same bucket). The default indexes directory (`.indexes`) is shared, so at least one environment must override it.

### Writer accumulation under high cardinality (Mitigated)

Each active `Writer` holds a `FormatWriter` (typically 1-10 MB of heap for Parquet/Avro page buffers and dictionary encoders), an open `java.io.File` handle (one file descriptor), and commit-state metadata. Under high-cardinality `PARTITIONBY` (e.g. partitioning by `user_id` or `session_id` with millions of unique values), the connector creates a `Writer` per unique value.

**Mitigation**: `WriterManager` eagerly evicts all idle writers (those in `NoWriter` state) whenever a new writer is created. Their `close()` is called, releasing the `FormatWriter` heap and file descriptor. The granular cache entry is deliberately **not** evicted -- it is left in the cache so that `cleanUpObsoleteLocks` can detect it as obsolete on the next `preCommit`. Writers in `Writing` or `Uploading` state are pinned and never evicted. See "Idle writer eviction" under "Lifecycle" for full details.

Note that eviction only applies to idle writers. If the workload simultaneously maintains a very large number of *active* writers (all in `Writing` state), the map grows without bound. This is an inherent constraint: active writers hold buffered data that cannot be discarded without data loss. Operators with such workloads must size heap and `ulimit -n` accordingly.

### GC timing in preCommit (High -- mitigated)

`cleanUpObsoleteLocks` runs inline during `preCommit` (only after a successful master lock update), but it now performs **no cloud I/O**. It evicts obsolete entries from the in-memory cache and enqueues their storage paths onto a `ConcurrentLinkedQueue`. Actual cloud deletes are performed asynchronously by a background `ScheduledExecutorService` that drains the queue at a configurable interval (`connect.<prefix>.indexes.gc.interval.seconds`, default 300s) and issues batched deletes (`connect.<prefix>.indexes.gc.batch.size`, default 1000 keys per call). This decouples GC latency from `preCommit` entirely, eliminating the risk of `offset.flush.timeout.ms` breaches under high cardinality. See "Garbage collection" under "Lifecycle" for full details.

### Orphaned granular lock files (Mitigated)

`cleanUpObsoleteLocks` only operates on lock entries present in the in-memory cache. Granular lock files from prior runs that no longer receive data are never loaded into the cache and therefore accumulate in cloud storage over time. With high-cardinality `PARTITIONBY` and evolving partition keys (e.g. date-based partitioning), the number of orphaned files grows with each key rotation.

**Mitigation**: An automatic periodic orphan sweep discovers and deletes these files. The sweep is configurable via three properties:

| Property | Default | Description |
|----------|---------|-------------|
| `connect.<prefix>.indexes.gc.sweep.enabled` | `true` | Enable or disable the orphan sweep. |
| `connect.<prefix>.indexes.gc.sweep.interval.seconds` | `86400` (24h) | Interval between sweep runs. Only effective when the sweep is enabled. |
| `connect.<prefix>.indexes.gc.sweep.min.age.seconds` | `86400` (24h) | Minimum age a lock file must have before the sweep considers it for deletion. Files younger than this are skipped without a GET read. Should be >= the sweep interval. |
| `connect.<prefix>.indexes.gc.sweep.max.reads` | `1000` | Per-cycle GET cap across all partitions. When exhausted, remaining partitions are deferred to the next cycle. |

See "Orphaned lock sweep" under "Garbage collection" for the full sweep architecture, filtering logic, and exactly-once safety analysis.

**Cost considerations**: LIST API calls are proportional to the number of owned partitions per sweep cycle (one call per partition). GET calls are globally capped at `gcSweepMaxReads`. Since the default sweep interval is 24 hours, the cost burst is infrequent. Operators with very high partition counts (thousands per task) should be aware of the LIST cost spike and can increase the sweep interval or disable the sweep if necessary.

**Known limitation -- marker orphans on task scale-down**: The sweep writes a marker file (`sweep-marker-<taskNo>.json`) into every distinct bucket the task owns partitions in. When the connector is scaled down (e.g. from 10 tasks to 5), marker files for tasks 5-9 remain in each bucket they previously wrote to. These are tiny JSON files (< 200 bytes each) and do not affect correctness or runtime behavior.

### Granular cache and writer map desync (Resolved)

Previously, the granular lock cache used a bounded `LinkedHashMap` with automatic LRU eviction. When the number of active partition keys exceeded the configured cache size, the cache silently evicted entries for active writers. On next flush, the writer detected the missing eTag and raised a `FatalCloudSinkError`, crashing a healthy task.

**Resolution**: The granular cache is now an unbounded `ConcurrentHashMap` with no automatic eviction. Entries are added when writers are created and removed by `cleanUpObsoleteLocks` (GC enqueue), `evictAllGranularLocks` (shutdown/rebalance), or explicit `evictGranularLock` calls. `Writer.close()` deliberately does **not** evict cache entries, allowing `cleanUpObsoleteLocks` to detect obsolete keys. Idle writers are eagerly evicted from the writers map whenever a new writer is created, keeping the map lean and allowing lock file GC to proceed promptly. See "Lazy loading and in-memory cache" and "Idle writer eviction" under "Lifecycle" for full details.

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
| Crash during garbage collection | Items already enqueued in `gcQueue` but not yet drained are lost. Items already drained but whose batched delete was in flight may be partially deleted. In both cases the orphaned granular lock files are harmless -- they sit below the `globalSafeOffset` watermark and are either re-enqueued on the next run or cleaned up by the periodic orphan sweep. **No impact on correctness.** |
| Crash with `PendingState` on a granular lock | `PendingState` is detected and resolved during **lazy load**, when the writer first calls `getSeekedOffsetForPartitionKey` for that partition key. If the local staging file still exists, the pending upload is completed. If the staging file is gone (the common case after a crash), the pending operation is abandoned (`cancelPending=true`) and the records revert to "uncommitted". On replay from the master lock offset, these records are re-delivered and re-processed. The write is **re-done from scratch**, not resumed. **No data loss, no duplication.** |
| Crash with `PendingState` on master lock | Existing behavior, unchanged. On restart, pending operations are completed. **No data loss.** |
| Zombie task uploads to `.temp-upload/` but Phase 2 eTag update fails | Temp file is orphaned at `.temp-upload/...`. Final output path is unaffected. **No duplication.** Storage leak requires periodic sweep (see "Zombie task and temp-upload fencing"). |
| Master lock update fails repeatedly at `preCommit` | `preCommit` returns no offset for the affected partition -- Kafka Connect does not advance the consumer offset. Master lock remains stale. GC is skipped (it only runs after a successful master lock update), so all granular locks are preserved. On crash, consumer replays from the stale offset. Granular locks deduplicate already-committed records. **No data loss, no duplication.** The eTag is not refreshed on failure -- for transient errors it remains valid and the next cycle succeeds; for eTag mismatches (fencing) repeated failure is the correct behavior, preventing a zombie task from advancing. |
| New record arrives for a previously evicted idle writer | The writer was in `NoWriter` state (committed, no buffered data) when evicted. A new writer is created, `ensureGranularLock` recreates the lock (or finds it still exists if GC hasn't run yet), and `getSeekedOffsetForPartitionKey` loads the offset from storage (or falls back to master lock offset if the lock was GC'd). `shouldSkip` correctly deduplicates already-committed records. **No data loss, no duplication.** |
| Idle writer with highest committed offset is evicted | The `safeOffsetHighWatermarks` map prevents `globalSafeOffset` from regressing below the previously reported value. The master lock retains the high watermark. GC decisions made at the previous (higher) threshold remain valid. On crash recovery, the consumer seeks to the non-regressed master lock offset and does not replay records whose granular locks were deleted. **No data loss, no duplication.** |
| GC enqueues lock for deletion, but new writer reclaims it before drain runs | `cleanUpObsoleteLocks` removes the cache entry and enqueues the file path. Before the background drain runs, a new writer calls `ensureGranularLock`, which re-reads the file from storage and re-populates the cache. When `drainGcQueue` runs, it checks `granularCache.containsKey(tp, pk)` and finds the reclaimed entry, skipping the delete. The new writer's eTag remains valid. **No data loss, no duplication.** |
| Crash during orphan sweep | The eTag-conditional write-before-sweep marker file prevents an immediate re-sweep on crash restart and prevents concurrent sweeps after a rebalance (only the task that wins the conditional write proceeds). Items already enqueued into `gcQueue` are subject to the same offset-gated and cache-gated deletion as regular GC items. Partially scanned partitions are deferred to the next sweep cycle. **No impact on correctness.** |
| Task restarts with higher master lock from previous instance | The high-watermark (`safeOffsetHighWatermarks`) is initialized on first `preCommit` from the master lock's `committedOffset + 1`. Even if the restarted task only sees partition keys with lower offsets, the `globalSafeOffset` floor is set from the persistent master lock, preventing regression. **No data loss, no duplication.** |

---

## Summary

| Concern | Before (single lock) | After (two-tier locks) |
|---------|---------------------|----------------------|
| **Duplication** | Writers overwrite each other's committed offset → offset regression → replayed records are re-written | Each writer has its own granular lock → no interference → `shouldSkip` is per-writer accurate |
| **Data loss** | `preCommit` returns max committed offset → Kafka advances past uncommitted buffered data → crash loses records | `preCommit` returns `min(firstBufferedOffset)` → Kafka never advances past uncommitted data |
| **Exactly-once** | Not guaranteed with PARTITIONBY | Guaranteed: no data loss (globalSafeOffset invariant) + no duplication (granular lock dedup) + monotonicity (globalSafeOffset never regresses) |
| **Writer accumulation** | N/A (single writer per partition) | Idle writers eagerly evicted on every new writer creation; active writers pinned. Cache grows/shrinks in lockstep -- no desync. |
| **Migration** | N/A | Fully transparent, zero-downtime upgrade |
| **Rollback** | N/A | Safe: master lock is backward-compatible, granular locks are ignored by old code |
