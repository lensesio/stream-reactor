# Tasks: LC-349

## Setup
- [ ] **T1: Create the branch**
  - Acceptance: `fix/LC-349-restage-swept-batch-temps` exists, based on a freshly fetched `origin/master`. Untracked local files (`SPEC.md`, `.claude/worktrees/`, `kafka-connect-tuning-email.html`) are not committed.
  - Verify: `git log -1` equals `origin/master`.
  - Files: none.

## S1: Re-stage (Bug A)
- [ ] **T2: IT harness and Case 1 "retry after sweep" (red)**
  - Acceptance:
    - New `S3BatchRestageAfterSweepTest`, built on `S3ProxyContainerTest`.
    - It uses an IT-only `S3SinkTask` subclass and an `AwsS3StorageInterface` subclass with two armable faults: on `uploadFile` by suffix, and on `mvFile` by source (persistent while armed). The state is reset in `beforeEach`.
    - Config:
      - batch mode, `error.policy=RETRY`;
      - KCQL `PARTITIONBY <field>` with `PROPERTIES('flush.count'=2)`;
      - sweep enabled, interval 1 s, minimum age 1 s.
    - Records on partition 1, at offsets 0, 1 and 2, routed A, B, B.
    - Steps:
      1. `open()`.
      2. Arm the fault on B's upload and call `put()`. It throws `RetriableException`. A's temp exists and B's does not.
      3. Wait with `eventually` until A's temp is gone.
      4. Disarm and call `put()` again.
    - Assertions:
      - both final files exist exactly once with the exact records;
      - none of the retry's temps remain;
      - the master lock has no `PendingState` and is at offset 2;
      - `preCommit` reports offset 3.
  - Verify: `sbt "project aws-s3" "it:testOnly *S3BatchRestageAfterSweepTest"` is **red**: a `FatalConnectException` on the retry, and a fresh task's `open()` fails with `…both missing`. Save the output.
  - Files: `kafka-connect-aws-s3/src/it/scala/io/lenses/streamreactor/connect/aws/s3/sink/S3BatchRestageAfterSweepTest.scala` (new).

- [ ] **T3: Unit tests for the re-stage contract (red)**
  - Acceptance:
    - `WriterStageTest`:
      - same id → 0 storage calls;
      - different id → 1 upload under the new id, with bucket, final path, offsets and count unchanged;
      - a namer that changes its answer still gets the frozen final path;
      - a failed re-upload leaves the old `Staged`;
      - a missing local file → `FatalCloudSinkError`.
    - `WriterCommitManagerBatchTest`: A's temp is deleted between attempts, and the retry commits both files.
  - Verify: `sbt "project cloud-common" "testOnly *WriterStageTest *WriterCommitManagerBatchTest"`. The new tests are red.
  - Files: `WriterStageTest.scala`, `WriterCommitManagerBatchTest.scala`.

- [ ] **T4: Re-stage fix and the allowed fixture updates**
  - Acceptance:
    - `Staged` gets a `batchUuid` field (set by `toStaged`) and `restaged(tempPath, eTag, batchUuid)`.
    - `Writer.stage` re-stages a `Staged` writer from another batch: frozen `(bucket, finalPath)`, no namer re-run, no HEAD check, and the in-flight metric stays balanced. On failure the state stays the old `Staged`.
    - Fixtures are updated only where the spec lists them, keeping every safety assertion.
  - Verify: `sbt "project cloud-common" test` is green, and IT Case 1 is green.
  - Files: `Writer.scala`, `WriteState.scala`, and the two test files.

**CP2:** commit `fix(cloud-sink): re-stage batch temps under the new batch id on retry (LC-349)`.

## S2: M′ (Bugs B and C)
- [ ] **T5: `BatchCommitLocks` and M′ sweep tests (red)**
  - Acceptance:
    - `BatchCommitLocksTest`:
      - the same lock instance is returned per partition across revoke and reassign;
      - entries are never removed;
      - the lock is fair;
      - the no-op implementation runs the body.
    - `BatchTempSweepTest` covers the eight deterministic scenarios in the spec:
      1. a held lock makes the delete wait;
      2. a CAS between the LIST and the delete spares the referenced temp;
      3. a sweep run during staging leaves A's fresh temp in place;
      4. with `gcBatchSize=1`, the commit acquires the lock between chunks;
      5. an owner mismatch from a second instance deletes nothing and logs a WARN;
      6. `acceptingWork=false`, or a revoked partition, deletes nothing;
      7. an unreadable fresh lock stops the pass;
      8. an interrupt while waiting issues no delete.
  - Verify: `sbt "project cloud-common" "testOnly *BatchCommitLocksTest *BatchTempSweepTest"`. The new tests are red, or fail to compile until T6 adds the class.
  - Files: `BatchCommitLocksTest.scala` (new), `BatchTempSweepTest.scala`, and `WriterCommitManagerBatchTest.scala` for scenario 3 if it fits better there.

- [ ] **T6: M′ implementation, wiring and owner fence**
  - Acceptance:
    - `BatchCommitLocks`: a `ConcurrentHashMap[TopicPartition, ReentrantLock(true)]`, filled with `computeIfAbsent` and never removed. It offers `withLock(tp)(f)` and `withLockInterruptibly(tp)(f)`, plus `NoOp`.
    - `WriterManagerCreator` builds one instance and passes it to `IndexManagerV2` and to `WriterManager`, which passes it on to `WriterCommitManager`. All of these are constructor parameters with a `NoOp` default.
    - `commitBatch` wraps stage → CAS → chain → finalize in `withLock(tp)`.
    - `sweepOrphansUnder` keeps the LIST and filter outside the lock. For each chunk, inside `withLockInterruptibly(tp)`:
      1. check `acceptingWork && seekedOffsets.contains(tp)`;
      2. read the master lock fresh, failing closed if it can't be read;
      3. fence on `owner == lockOwner`, logging a WARN and stopping the pass on a mismatch;
      4. drop the entries the fresh lock now references;
      5. call `deleteFiles`.
    - The lock is never held across `IO`.
    - Comments state the invariant and the bounded commit wait.
  - Verify: `sbt "project cloud-common" test` is green, including the Mockito-based batch tests, which stay unchanged.
  - Files: `BatchCommitLocks.scala` (new), `IndexManagerV2.scala`, `WriterCommitManager.scala`, `WriterManager.scala`, `WriterManagerCreator.scala`.

**CP3:** commit `fix(cloud-sink): serialise batch temp deletes with commits and fence stale sweeps (LC-349)`.

## S3: Restart variants
- [ ] **T7: IT Cases 2a and 2b, and unit AC3c**
  - Acceptance:
    - **2a:** the retry's copy of A fails; the referenced temps survive at least 2 sweep intervals; a fresh task's `open()` resumes; each file lands once; re-delivery creates no extra files.
    - **2b:** B's copy fails after A has landed; on restart B lands, A is not rewritten (same eTag), and nothing is re-uploaded.
    - **Unit AC3c:** a lock-update failure after a copy is Fatal and the writers are discarded under NOOP; a restart completes without re-uploading.
  - Verify: the IT and unit suites are green. With `Writer.scala` reverted locally, 2a is red; then restore it.
  - Files: `S3BatchRestageAfterSweepTest.scala`, `WriterCommitManagerBatchTest.scala`.

**CP4:** commit `test(aws-s3): restart paths after a re-staged batch commit (LC-349)`.

## S4: Docs
- [ ] **T8: Update `docs/` exactly as the spec's "Docs (AC9)" section says**
  - Acceptance:
    - Write-pipeline: the table rows, line 204 and line 364 are updated.
    - Partitionby:
      - the LC-349 subsection covers Bugs A, B and C, re-stage, M′ and the owner fence;
      - lines 772 and 893 are corrected;
      - the full repair runbook is added.
    - Test-coverage: new rows, line 324 updated, and the orphan residual updated.
  - Verify: every relative link and every `file:line` reference resolves on the branch.
  - Files: the three docs.

**CP5:** commit `docs: batch re-staging, sweep/commit serialisation and stuck-task runbook (LC-349)`.

## S5: Ship
- [ ] **T9: Full verification and PR draft**
  - Acceptance:
    - `sbt headerCreateAll scalafmtAll` leaves no diff, `validateAll` is clean, cloud-common `test` is green, and `aws-s3 it:test` is green.
    - The diff review confirms no safety assertion was weakened.
    - The attribution lines are on every commit.
    - The PR text follows the spec outline, including the alternatives considered.
    - **I ask before pushing or opening the PR.**
  - Verify: the commands above, and you review the PR text.
  - Files: none new.
