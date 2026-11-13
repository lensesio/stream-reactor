# Spec: LC-349 — re-stage batch temps on retry, and serialise the batch-temp sweep with commits

Status: **v3, awaiting re-approval** (2026-11-13).
- v1 was approved.
- v2 folded in the sol-loop review.
- v3 replaces v2's in-flight registry with a **per-partition mutex plus a per-chunk fresh read (M′)**, after an Opus validation review. All review claims were verified against `origin/master` e5d3e3739.

Ticket: [LC-349](https://landoop.atlassian.net/browse/LC-349) (Bug, Medium). Branch: `fix/LC-349-restage-swept-batch-temps`, cut from `origin/master` (local `master` is stale). The bug is independent of PR 413.

## Objective

**How batch commit works.** In `exactly.once.commit.mode=batch`, `WriterCommitManager.commitBatch(tp)` does five things:
1. It generates a fresh `batchUuid`.
2. It runs `Writer.stage(batchUuid)` **serially** on every non-idle writer. Each call uploads to `.temp-upload/<connector>/<topic>/<partition>/<batchUuid>/<finalPath>`, and the writer becomes `Staged`.
3. Only if every stage succeeded, it writes the master lock with a CAS. The lock gets a `PendingState` of `CopyOperation`s. That CAS is the only commit point.
4. It runs the copy chain (`mvFile` moves each object). After each successful copy it rewrites the lock with the remaining ops.
5. It finalizes the writers.

The only caller is `commitBatches`. It runs only on the task thread and is never re-entrant.

**How the sweep works.** `IndexManagerV2.sweepBatchTemps(tp)` does three things:
1. It reads the master lock's referenced copy sources.
2. It lists the prefix. The S3 listing is paginated.
3. In chunks of `gcBatchSize`, it deletes every object at or before `now − gc.sweep.min.age.seconds` (default 24h) that is not referenced (`IndexManagerV2.scala:395–471`).

It runs at `open()` (on cats-effect fibers, after `PendingState` recovery) and, when `indexes.gc.sweep.enabled` is set, periodically on its own thread (`sweepAllBatchTemps`). `gcSweepMaxReads` does **not** bound this sweep. Nothing else lists or deletes the batch prefix.

**Bug A: a retry trusts a stale temp (the ticket).**
1. Writer B's stage upload fails with a non-fatal error. Writer A is already `Staged`, and nothing references its temp.
2. The sweep deletes A's temp.
3. The retry (`recommitPending` → `commitBatch`) reuses A's old path, because `stage()` on a `Staged` writer is a no-op (`Writer.scala:180`).
4. The CAS records a copy whose source and destination are both missing. All three backends return `FileMoveError`, which becomes `FatalCloudSinkError`.
5. `open()` replays the same copy on every restart. **The task is stuck until someone repairs it by hand.**

**Bug B: the sweep races a live commit (found in review).** There are two windows:
- **Before the CAS:** a fresh temp ages past the minimum age while later writers upload. Clock skew or coarse `lastModified` timestamps make this more likely.
- **Across the CAS:** a sweep that read the lock before the CAS deletes a temp the CAS has just referenced.

With the default 24h minimum age this needs a day-long commit or a day of clock skew. With a low tuned value it is realistic.

**Bug C: stale or zombie sweeps (found in review, exists today).** The temp prefix is shared by every instance of the connector (`connectorTaskId.name`). A sweep thread can outlive `close()`, which waits only 5 s (`IndexManagerV2.scala:1867`). On a rebalance, `acceptingWork` is checked once per tick, not per partition. A zombie task on another worker sweeps too. Each of these can delete temps that belong to the current owner. Only the minimum age protects them today.

**When users hit Bug A:**

| `error.policy` | Affected? |
|---|---|
| THROW (default) | No. The failure restarts the task, which discards `Staged`. |
| RETRY | Only when the sweep age is lowered, or when `max.retries × retry.interval` exceeds it. |
| NOOP | Yes, with defaults, if one file's upload keeps failing for more than 24h. |

### The fix

**1. Re-stage (fixes Bug A).**
- `stage(batchUuid)` on a writer that is still `Staged` from a *different* batch re-uploads its local staging file to a fresh temp under the new id, before the CAS. The local file is kept until `finalizeCommit`.
- It keeps the frozen `(bucket, finalPath)` and does not re-run `objectKeyBuilder`. A custom `FileNamerFactory` (`CloudSinkBucketOptions.scala:136`) may answer differently on a second call.
- It does not HEAD-check the old temp, because that would race the sweep.
- If the re-upload fails, the state stays the old `Staged`.
- The old temp becomes an ordinary orphan.

**2. M′: serialise deletes with commits per partition (fixes Bug B).**
- **The lock map.** A `BatchCommitLocks` holds one **fair** `ReentrantLock` per topic-partition. Entries are created with `computeIfAbsent` and **never removed**. Removing one on revoke could hand a later commit a second lock while an old sweep still holds the first. Memory stays bounded by the partitions ever assigned.
- **Ownership.** `WriterManagerCreator` creates one instance and passes it to both `IndexManagerV2` and `WriterManager` → `WriterCommitManager`. Constructor parameters default to a no-op, so granular mode, `NoIndexManager` and Mockito-mocked `IndexManager` tests keep working. No higher-order trait method is added.
- **Commit side.** `commitBatch(tp)` holds the lock with a blocking `lock()` across stage → CAS → copy chain → finalize, and releases it in `finally`.
- **Sweep side: read and list outside the lock.** The lock GET, the LIST and the age/reference filtering stay *outside* the lock, as today.
- **Sweep side: per delete chunk.**
  1. Acquire the lock with `lockInterruptibly()`. The sweep thread may wait, and `shutdownNow` interrupts it.
  2. Re-check that `acceptingWork` is set and the partition is still in `seekedOffsets`.
  3. Re-read the master lock and drop any chunk entries it now references. If the read fails, skip the rest of the pass (fail closed, as today).
  4. Run `deleteFiles(chunk)`.
  5. Release the lock.
- **Bounded commit wait.** A commit waits at most **one master GET plus one `deleteFiles` call of up to `gcBatchSize` keys**, never a whole sweep pass.
- **Why it is correct.** While the sweep holds the lock, no commit is in flight on that partition. A temp listed earlier is in one of these states:
  - from a commit that has finished: already moved, and deleting a missing key does nothing;
  - from a commit that failed before the CAS: a true orphan, because the retry re-stages under a new id;
  - from a commit that failed after the CAS: referenced by the fresh read, so it is dropped from the chunk.

  Temps uploaded after the LIST are not in the listing.
- **Threading.** The lock is only ever taken and released inside one synchronous thunk, never across an `IO` boundary.
- **Ownership signal (revised after S2; replaces the `requireLiveOwnership` flag).**
  - `IndexManagerV2` keeps an explicit concurrent `ownedPartitions` set.
    - `open(tp)` adds `tp` at its start, before recovery and the `open()` sweep.
    - A new `IndexManager.releasePartitions(tps)` hook, a no-op by default, removes them. It is called from `CloudSinkTask.close(partitions)` next to `suspendBackgroundWork()`, and the set is cleared on `stop`/`close`.
    - `open()`'s pruning of revoked partitions also removes them.
  - Before each chunk, the per-chunk check is `ownedPartitions.contains(tp)`, the same check for both the `open()` and the periodic sweep. The flag and the per-chunk `acceptingWork`/`seekedOffsets` check are removed.
  - The scheduler's existing `if (acceptingWork)` gate on the periodic tick is unchanged.
  - Why: `seekedOffsets` means "has a committed offset", not "currently held", and `acceptingWork` is false throughout `open()`. A direct signal works at startup without a special case.
- **Missing or 0-byte lock on the fresh read (decided):** delete the chunk, as the code did before S2. Temp paths are unique per attempt (they include a random batch id), an in-flight commit holds the partition lock, and another process cannot commit without the lock. So an unreferenced temp is a true orphan. Any other read error still fails closed.
- **Known pre-existing gap.** The periodic sweep only visits partitions in `seekedOffsets`, which means partitions with a committed master offset. A partition that never committed has its orphans reaped only at `open()`. This is documented, not changed.

**3. Owner fencing on the fresh read (fixes Bug C).** A chunk is deleted only if the fresh master lock's `owner == lockOwner`. `lockOwner` is `ConnectorTaskId.lockUuid`, unique to each instance, and `open()` always bumps the owner (`IndexManagerV2.scala:570, 634`). On a mismatch the pass stops for that partition and logs a WARN: "lock owned by another instance; batch temp sweep skipped". So a stale or zombie sweep never deletes the current owner's temps.

**Who benefits.** Operators of the S3, GCS and Azure Data Lake sinks with `commit.mode=batch`, which is the supported mode for wall-clock PARTITIONBY.

### Acceptance criteria
- **AC1.** A batch retried after a partial stage failure commits every file exactly once, with no loss, even when the sweep deleted the earlier temps between attempts.
- **AC2.** No sweep deletes a temp of a batch that this task is staging or committing. That holds for old, unreferenced temps, and for a sweep whose listing came before the CAS.
- **AC3.** Restart paths recover with no loss and no duplicates:
  - (a) the retry's first copy fails;
  - (b) a later copy fails after an earlier file landed;
  - (c) a lock update fails after a successful copy.

  The live task fails and discards its writers under any `error.policy`. A fresh `open()` resumes from the persisted `PendingState` without re-uploading or rewriting files that already landed.
- **AC4.** A re-staged writer keeps its original `(bucket, finalPath)`, offsets and record count.
- **AC5.** A commit never waits on the sweep for more than one master GET plus one `deleteFiles` chunk.
- **AC6.** A sweep whose instance no longer owns the partition's master lock (stale, zombie, or revoked) deletes nothing.
- **AC7.** Granular mode is unchanged, and so is every batch path that is not a retry of a partially staged batch: still one upload per writer per batch.
- **AC8.** The new IT is red on unfixed `master` and green with the fix.
- **AC9.** `docs/` describe Bugs A, B and C, the fix, and a rigorous field-repair runbook.

## Tech Stack
Scala 2.13, sbt. Cats `Either`, cats-effect (in `open()`). `java.util.concurrent.locks.ReentrantLock(true)` and `ConcurrentHashMap`. ScalaTest. Testcontainers with S3Proxy. No new dependencies.

## Commands
```
git fetch origin && git switch -c fix/LC-349-restage-swept-batch-temps origin/master

sbt "project cloud-common" "testOnly *WriterStageTest *WriterCommitManagerBatchTest *BatchTempSweepTest *BatchCommitLocksTest *BatchCommitScenarioTest"
sbt "project cloud-common" test

sbt "project aws-s3" "it:testOnly *S3BatchRestageAfterSweepTest"   # needs Docker
sbt "project aws-s3" it:test

sbt headerCreateAll scalafmtAll
sbt validateAll
```

## Project Structure (files in scope)
```
cloud-common/.../sink/writer/Writer.scala              → stage(): re-stage a Staged writer from another batch (frozen bucket/finalPath)
cloud-common/.../sink/writer/WriteState.scala          → Staged carries batchUuid; Staged.restaged(tempPath, eTag, batchUuid)
cloud-common/.../sink/seek/BatchCommitLocks.scala      → NEW: per-tp fair ReentrantLock map (computeIfAbsent, never removed) + NoOp
cloud-common/.../sink/writer/WriterCommitManager.scala → ctor param batchCommitLocks (default NoOp); commitBatch holds tp lock in try/finally
cloud-common/.../sink/writer/WriterManager.scala       → ctor param passed through to WriterCommitManager (default NoOp)
cloud-common/.../sink/WriterManagerCreator.scala       → create one BatchCommitLocks; pass to IndexManagerV2 and WriterManager
cloud-common/.../sink/seek/IndexManagerV2.scala        → ctor param; sweepOrphansUnder: per-chunk lock + acceptingWork/seekedOffsets check + fresh read + owner fence
cloud-common/src/test/.../BatchCommitLocksTest.scala (NEW), BatchTempSweepTest.scala, WriterStageTest.scala, WriterCommitManagerBatchTest.scala
aws-s3/src/it/.../aws/s3/sink/S3BatchRestageAfterSweepTest.scala   → NEW IT
docs/datalake-sinks-write-pipeline.md, docs/datalake-exactly-once-partitionby.md, docs/datalake-sink-test-coverage.md
```
No change to `mvFile`, config keys or defaults, granular paths, the `IndexManager` trait, or `open()` recovery.

## Code Style
Follow the surrounding code: `Either`/`asRight`, transitions on `WriteState` with a `logger.debug("state transition: …")`, comments that explain *why*, and LC-349 cited where the reason is non-obvious. Sketches:

```scala
// Writer.stage
case s: Staged if s.batchUuid == batchUuid => return s.some.asRight
case s: Staged =>
  // LC-349: leftover from an earlier, partially failed commitBatch; nothing references its temp,
  // so the sweep may have deleted it. Re-upload the local file (kept until finalizeCommit) under
  // the new batch id. Destination is frozen: never re-run the namer. No HEAD check (races the sweep).
  val tempPath = tempPathFor(batchUuid, s.finalPath)
  pendingOperationsProcessors.uploadOnly(topicPartition, UploadOperation(s.bucket, s.file, tempPath), partitionKey, Some(s.file))
    .map { eTag => val r = s.restaged(tempPath, eTag, batchUuid); writeState = r; r.some }

// IndexManagerV2.sweepOrphansUnder, per chunk (LIST + filter already done outside the lock)
batchCommitLocks.withLockInterruptibly(tp) {
  // LC-349: under the tp lock no commit is in flight, so a fresh read is authoritative for every
  // CAS that landed after our LIST. Owner fence: never delete for a lock this instance doesn't own.
  if (!acceptingWork || !seekedOffsets.contains(tp)) StopSweep
  else freshMaster(tp) match {
    case Right(m) if m.owner != lockOwner => StopSweep            // stale/zombie instance
    case Right(m)                         => deleteFiles(bucket, chunk.filterNot(referencedBy(m)))
    case Left(_)                          => StopSweep            // fail closed
  }
}
```

## Testing Strategy
Tests are written first and observed **red on unfixed code** (Prove-It). The concurrency tests are deterministic: they use latches and storage hooks, with no sleeps or timers.

**Integration test: `S3BatchRestageAfterSweepTest` (S3Proxy).**
- **Fault seam, IT-only.**
  - An `S3SinkTask` subclass overrides `createStorageInterface` to return an `AwsS3StorageInterface` subclass.
  - It can be armed to fail `uploadFile` by final-path suffix.
  - It can be armed to fail `mvFile` by source, persistently while armed, because `RetryingStorageInterface` retries `mvFile`.
  - The test controls the armed state and resets it in `beforeEach`.
- **Exact config.**
  - batch mode, `error.policy=RETRY`;
  - KCQL `PARTITIONBY <field>` with `PROPERTIES('flush.count'=2)`, other thresholds high;
  - `indexes.gc.sweep.enabled=true`, interval 1 s, minimum age 1 s (`min.age` must be at least the interval).
- **Records.** Partition 1, offsets 0, 1 and 2, routed A, B, B. The expected end state is master offset `2` and `preCommit` offset `3`.
- **Order and errors.** `open()` must succeed before relying on the scheduled sweep. Fatal errors surface as `FatalConnectException`.
- **Case 1, retry after sweep (AC1, AC8).**
  1. Arm the fault on B's upload and call `put()`. It throws `RetriableException`. A's temp exists and B's does not.
  2. Wait with `eventually` (bounded, ≥ 10 s) until A's temp is gone.
  3. Disarm and call `put()` again.
  4. Assertions:
     - both final files exist exactly once, with the exact records;
     - none of the retry's temps remain;
     - the master lock has no `PendingState` and is at offset 2;
     - `preCommit` reports offset 3.
  5. Red on unfixed code: a dangling copy is persisted, and a fresh task's `open()` fails with `…both missing`.
- **Case 2a, the retry's first copy fails (AC3a).**
  1. Arm the `mvFile` fault on the retry's temp of A. The retry fails fatally after the CAS.
  2. The referenced temps survive at least 2 sweep intervals.
  3. Stop and disarm. A fresh task's `start` and `open` resume the chain.
  4. Each file lands exactly once, and re-delivery creates no extra files.
  5. On unfixed code, the red evidence is that A's persisted source is missing on recovery.
- **Case 2b, a later copy fails after A landed (AC3b).** Arm the fault on B's temp. On restart B lands, A is not rewritten (same eTag), and nothing is re-uploaded.

**Unit tests, `WriterStageTest` (AC4):**
- the same id makes 0 storage calls;
- a different id makes 1 upload under the new id, with bucket, final path, offsets and count unchanged;
- a namer that changes its answer on the second call still gets the frozen final path;
- a failed re-upload leaves the old `Staged`;
- a missing local file gives `FatalCloudSinkError`.

**Unit tests, `WriterCommitManagerBatchTest`:**
- A's temp is deleted between attempts, and the retry commits both files (AC1);
- a lock-update failure after a copy is Fatal, the writers are discarded under NOOP too, and a restart completes without re-uploading (AC3c).

**Unit tests, `BatchCommitLocksTest`:**
- the same instance is returned for a partition across revoke and reassign;
- entries are never removed;
- the no-op implementation runs the body;
- the lock is fair.

**Unit tests, `BatchTempSweepTest`, deterministic M′ (AC2, AC5, AC6):**
1. **Commit holds the lock, so the sweep waits.** A test-held partition lock blocks the sweep's delete. After release the delete runs, minus anything now referenced.
2. **CAS between the LIST and the delete.** A hook after `listFileMetaRecursive` runs a CAS that references a listed temp. The delete spares it.
3. **Sweep during staging.** A storage hook on B's upload starts the sweep on another thread with minimum age 0. A's fresh temp survives, the commit completes, and the sweep finishes afterwards.
4. **Bounded wait.** With `gcBatchSize=1` and 3 orphans, a commit that starts after the first chunk acquires the lock before the remaining chunks. The interleaving is asserted with a latch.
5. **Owner fence.** A second `IndexManagerV2`, with a different `lockUuid` and sharing the storage, runs `sweepAllBatchTemps` after the first instance's `open()` has bumped the owner. It deletes nothing and logs a WARN.
6. **Revoked or suspended.** With `acceptingWork=false`, or the partition missing from `seekedOffsets`, the sweep deletes nothing.
7. **Fail closed.** An unreadable master lock on the fresh read stops the pass.
8. **Interrupt.** `close()` and `shutdownNow` interrupt a sweep waiting on the lock, and no delete is issued.

**Fixture updates allowed.** Tests that pre-stage with `"probe"` and then call `commitBatch` may be updated to capture eTags and arm faults against the *current attempt*. Every safety assertion is kept. The tests are:
- `WriterCommitManagerBatchTest` 303–320, 385–397 (rewritten to the new retry contract), 446–459 and 481–498;
- `WriterStageTest` `[ND] … idempotent` (rewritten to the same-id/different-id contract).

`BatchCommitScenarioTest`, the granular suites, and the Mockito-based batch tests stay unchanged (AC7).

**Docs (AC9).** Every edit listed here is authorized.
- **`datalake-sinks-write-pipeline.md`:**
  - In the batch failure table, the `stage()` transient row describes re-staging with a frozen destination.
  - The "last Copy failure" row becomes Fatal, with a restart that resumes via `open()`.
  - The stale "post-commit Delete" row is removed.
  - The paragraph near line 204 and the retry paragraph at line 364 are aligned.
- **`datalake-exactly-once-partitionby.md`:**
  - New subsection "Batch temps, retries and the orphan sweep (LC-349)": Bugs A, B and C, reachability, re-stage, M′ (lock scope, per-chunk fresh read, bounded commit wait) and owner fencing.
  - Lines 772 and 893 are corrected to the real sweep gating.
  - **Field-repair runbook:**
    1. Recognise the stuck task: the `mvFile: both source … and destination … are missing` log, plus a `PendingState` whose `.temp-upload` sources are absent.
    2. Stop the connector, then back up the master lock and the affected final objects.
    3. Inventory the files already landed, from the final paths and the offset range. The remaining `PendingState` lists only the ops not yet done.
    4. Confirm Kafka still retains the records from the committed floor.
    5. Clear the `PendingState` **without advancing the committed offset**.
    6. If replay is impossible, escalate.
    7. Caveats: duplicates are possible, GCS may replace a file (generation-match precondition), and under wall-clock keys replayed records may land in different files.
- **`datalake-sink-test-coverage.md`:** new rows for the IT and unit tests, line 324 updated, and the orphan-temps residual updated (up to N orphans per failed attempt, reaped when unreferenced).

## Boundaries
- **Always:**
  - Write the IT first and observe it red.
  - Unit tests and the aws-s3 ITs are green before each commit.
  - Run `headerCreateAll scalafmtAll`.
  - Granular mode stays unchanged.
  - Never hold a `BatchCommitLocks` lock across an `IO` boundary or a blocking wait on another thread.
  - Commits and the PR end with the attribution lines.
- **Approved by you (previously ask-first):**
  - M′ changes in `IndexManagerV2.sweepOrphansUnder`, `WriterCommitManager`, `WriterManager` and `WriterManagerCreator`;
  - the owner fence;
  - the listed test fixture updates and doc edits.
- **Ask first:**
  - any other change to the sweep, recovery or `open()`;
  - `mvFile` changes;
  - new config keys or defaults, or new metrics;
  - test changes beyond those listed;
  - pushing or opening the PR.
- **Never:**
  - auto-recover stuck `PendingState`s;
  - HEAD-check old temps;
  - re-run the namer on re-stage;
  - delete the local staging file before `finalizeCommit`;
  - remove lock-map entries;
  - hold the lock across a whole sweep pass;
  - weaken safety assertions.

## PR description (outline)
1. **The problem as chains of events.**
   - Bug A: partial stage → sweep → stale reuse → dangling copy → fatal on every restart.
   - Bug B: the sweep races a live batch before or across the CAS.
   - Bug C: stale or zombie sweeps.
   - Include the reachability table and why THROW is safe.
2. **The change.**
   - Re-stage with a frozen destination.
   - M′: the commit holds the partition lock; the sweep lists outside it and deletes per chunk under it, after a fresh read; the commit waits at most one chunk.
   - Owner fencing.
   - Why there is no HEAD check, no namer re-run, and no whole-pass lock.
3. **Cost.**
   - One extra upload per already-staged file on retries.
   - Old temps become orphans.
   - One extra master GET per delete chunk.
   - A commit can wait for at most one chunk.
4. **Proof.** The IT cases, the red and green evidence, the deterministic concurrency tests, and the fixture updates with reasons.
5. **Out of scope.** Auto-repair, with a link to the runbook. Also note the alternatives considered (in-flight registry, sweeping on the task thread, dropping the periodic sweep) and why M′ was chosen.
6. **Docs updated.**

## Success Criteria
- AC1–AC9 hold.
- `sbt "project cloud-common" test` and `sbt "project aws-s3" it:test` are green, and `validateAll` is clean.
- The IT was demonstrably red on unfixed code, and the PR shows the failure text.

## Resolved
- **`mvFile` with both source and destination missing:** S3, GCS and Azure all fail loudly. On GCS, an existing destination gets a generation-match precondition.
- **Field auto-repair:** out of scope; docs runbook only.
- **Stale doc rows:** fix them.
- **Branch:** `fix/LC-349-restage-swept-batch-temps`.
- **Can a writer stay `Staged` after a CAS while the task is alive?** No. Every post-CAS failure is Fatal, cleanup discards the writers, and NOOP does not swallow `FatalConnectException`. Verified three times.
- **Sweep race design:** M′ over the in-flight registry. M′ serialises sweeps with commits, reads fresh before every delete, fences other owners, has no lifecycle state and is less code. Its cost is a commit wait of at most one chunk.
- **Is `commitBatch` reachable with `NoIndexManager`?** No. Without index options the mode defaults to granular (`WriterManagerCreator.scala:140–160`). The no-op lock default is there for tests and safety.
