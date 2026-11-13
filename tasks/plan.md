# Plan: LC-349 — re-stage batch temps on retry, and serialise the sweep with commits (M′)

Spec: [`SPEC-LC-349.md`](../SPEC-LC-349.md) (v3).
Branch: `fix/LC-349-restage-swept-batch-temps`, from `origin/master`.

## Approach
Prove-It, in vertical slices. Each slice ends green and can be committed alone:

| Slice | Content |
|---|---|
| S1 | Re-stage (Bug A), from the IT to green |
| S2 | M′ lock and owner fence (Bugs B and C) |
| S3 | Restart variants |
| S4 | Docs |
| S5 | Verification and PR |

## Verified facts (origin/master e5d3e3739)
| Question | Answer | Where |
|---|---|---|
| Can the IT inject faults without production changes? | Yes. `createStorageInterface` can be overridden, and `AwsS3StorageInterface` is not final. | `S3SinkTask.scala:47`, `AwsS3StorageInterface.scala:62` |
| Does a retry wrapper hide faults? | It retries `mvFile` but not `uploadFile`. | `CloudSinkTask.scala:404` |
| Can a writer stay `Staged` after a CAS while the task is alive? | No. All post-CAS failures are Fatal, and cleanup discards the writers. | `PendingOperationsProcessors`, `commitBatch` |
| Is the final path stable on re-stage? | Only if the namer is not re-run, because a custom `FileNamerFactory` exists. | `CloudSinkBucketOptions.scala:136` |
| Who calls `commitBatch`? | Only `commitBatches`, on the task thread, never re-entrantly. | `WriterCommitManager.scala:111,124,134,153,165` |
| Who sweeps the batch prefix? | `open()` (on fibers, after recovery) and the periodic thread. Nothing else lists or deletes it. | `IndexManagerV2.scala:262,287,323–380` |
| Is the batch sweep bounded? | No. It does a full paginated LIST and every delete chunk; `gcSweepMaxReads` only applies to the granular sweep. Hence the lock is taken per chunk. | `IndexManagerV2.scala:444–471,1590` |
| Is there an owner identity to fence on? | `lockOwner` is `ConnectorTaskId.lockUuid`, unique per instance, and `open()` bumps the owner. | `IndexManagerV2.scala:99,570,634` |
| How long does `close()` wait for the sweep thread? | 5 s, so the sweep can outlive the instance. Hence the owner fence and the per-chunk ownership re-check. | `IndexManagerV2.scala:1864–1879` |
| How is `WriterCommitManager` constructed? | By `WriterManager`, from `WriterManagerCreator`, which also builds `IndexManagerV2`. That is where the shared lock map gets wired. | `WriterManager.scala:128`, `WriterManagerCreator.scala:140` |
| Can the IT use a minimum age of 1 s? | Yes, provided `min.age` is at least the interval. | `IndexManagerV2.scala:94` |

## Dependency graph
```
T1 branch
 └─ S1: T2 IT Case 1 (red) ┐
        T3 unit re-stage (red) ┴─ T4 re-stage fix + fixture updates (green)
     └─ S2: T5 BatchCommitLocks + M′ sweep tests (red) ─ T6 M′ impl + wiring + owner fence (green)
         └─ S3: T7 IT Case 2a/2b + unit AC3c
             └─ S4: T8 docs
                 └─ S5: T9 verification + PR draft (ask before push)
```

## Risks and mitigations
| Risk | Mitigation |
|---|---|
| A deadlock between the task thread and the sweep thread. | The commit never waits on the sweep thread. The sweep never waits on the task thread except for the lock. The lock is never held across `IO` or I/O-unrelated waits. The `close()` and `shutdownNow` interrupt path is tested. |
| A commit stalls behind the sweep. | The lock is held per chunk only. A test asserts the commit acquires the lock between chunks (AC5). |
| Sweep starvation. | A fair lock and a blocking `lockInterruptibly` on the sweep thread. There is no `tryLock` skip. |
| Lock-map entries are lost on revoke, so mutual exclusion is silently lost. | Entries are never removed, created with `computeIfAbsent`, and a unit test pins this. |
| A zombie or stale sweep deletes the owner's temps. | The owner fence plus the `acceptingWork`/`seekedOffsets` re-check on every chunk. A unit test runs two instances against shared storage. |
| Mockito-mocked `IndexManager` tests break. | No trait change. The lock map is a constructor parameter with a no-op default. |
| The IT flakes on the periodic sweep. | Bounded `eventually` only. Re-staged temps are protected by M′ even with a 1 s minimum age. |
| The IT turns out green on unfixed code. | CP1 requires observing it red and saving the failure text. |

## Checkpoints
- **CP1:** T2 and T3 are red on unfixed code, and the failure text is saved.
- **CP2 (S1):** cloud-common `test` and IT Case 1 are green. Commit.
- **CP3 (S2):** the M′ tests are green, and nothing else regresses (including the Mockito batch tests). Commit.
- **CP4 (S3):** Cases 2a, 2b and AC3c are green, and 2a is red with `Writer.scala` reverted. Commit.
- **CP5 (S4):** the doc links and `file:line` references resolve. Commit.
- **CP6 (S5):** `aws-s3 it:test` is green and `validateAll` is clean. You review the PR text before anything is pushed.
