# Elasticsearch / OpenSearch: per-record DLQ for permanent bulk item errors

This document describes the behaviour of the `bulk.item.errors.dlq` setting shared by the
Elasticsearch (6/7) and OpenSearch sink connectors. The connector config reference keeps only a
short summary; the full semantics live here.

## What it does

Controls what happens to a bulk item that fails with a **permanent, per-document error**
(HTTP 400 — e.g. a mapper conflict) once a real Kafka Connect dead-letter queue is available.

| Value            | Behaviour                                                                                                                                                             |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fail` (default) | The connector throws. Kafka Connect kills the task with offsets uncommitted, so the whole batch is redelivered on restart. No data is lost, but the task stops until restarted. |
| `skip`           | The failing record(s) are reported to the dead-letter queue and the batch is treated as handled so offsets advance past them, keeping the task running.                |

`skip` means *"a poison document does not kill the task"* — **not** *"the task can never fail"*.

## When does `skip` actually take effect

`skip` is ignored (and the connector behaves as `fail`) unless **all** of the following hold:

- **`errors.deadletterqueue.topic.name` is set** to a non-empty topic name. A non-null
  `ErrantRecordReporter` is not sufficient — Kafka Connect hands one out whenever a DLQ topic is
  set *or* `errors.log.enable=true`.
- **`errors.tolerance=all`.** Under `errors.tolerance=none`, Kafka Connect throws out of the
  report call once the record has been handed to the DLQ producer, so `skip` cannot keep the task
  running (the record still reaches the DLQ, but the task then fails fast).
- **`bulk.strict.item.errors=true`** (the default). In tolerant mode, per-item bulk failures are
  never surfaced, so there is nothing to report.

Misconfigurations here produce a WARN at task `start()` rather than a silent no-op discovered later.

## Only per-document rejections are diverted

`skip` diverts **only per-document rejections (HTTP 400**, e.g. a mapper conflict). Batch-wide
conditions still fail the task even in `skip` mode, because they are not caused by the content of
any single record — diverting them would discard good records while the task kept running and
reported itself healthy. These include:

- write-blocked index (403, `cluster_block_exception`)
- authorization failure (403, `security_exception`)
- missing index (404, `index_not_found_exception`)

Status (not error type) is what is checked, so a renamed/new document-level error type stays
eligible without a connector release.

## "If a batch of 100 records fails, do all 100 go to the DLQ?"

**No.** The DLQ only ever receives the individual records the cluster rejected with a per-document
HTTP 400 — never a whole batch wholesale. For a 100-record batch:

- **All 100 fail with a transport-level error** (bulk call timed out / connection reset / cluster
  returned a 5xx for the whole request): **0 go to the DLQ.** There is no per-document response
  telling us which records are bad, so diverting them would dump 100 perfectly-good documents.
  RETRY re-sends all 100; NOOP/THROW fail-fast.
- **Batch-wide permanent error** (e.g. 403 write-blocked, 404 missing index): **0 go to the DLQ.**
  The whole batch is thrown and redelivered — nothing is diverted.
- **3 rejected with HTTP 400, rest fine** (`skip` + DLQ topic): **only those 3 go to the DLQ**,
  are dropped from the batch, and the other 97 are written normally.
- **3 rejected with 400 + some 429 (retriable) in the same response**: the 3 go to the DLQ, and
  the batch still throws for the 429 remainder → redelivered. On redelivery the same 3 are
  reported again (at-least-once; dedup is per-`put()`).

The only way a large number of records reaches the DLQ is if the cluster genuinely rejected each
of them individually with a 400 — the intended "poison record" case. A single infrastructure blip
or a batch-wide misconfiguration never floods the DLQ.

## At-least-once delivery and mixed batches

DLQ delivery is at-least-once. If a batch mixes a permanent (DLQ-eligible) error with a retriable
one, the permanent record(s) are reported and the batch still fails for the retriable remainder,
so the same permanent record(s) are reported again on every redelivery attempt until the retriable
remainder clears. This matches Kafka Connect's own DLQ semantics.

The dedup that prevents double-reporting is scoped to a single `put()` (a fresh tracker is created
per `write()`), keyed by `(topic, partition, offset, target)`. It deliberately does **not** survive
across a redelivered `put()`.

## Fan-out to multiple KCQL targets

A record that fans out to more than one KCQL target may be committed successfully to one index and
reported to the DLQ for another; anything consuming the DLQ should expect to see records that were
also partially written.

## Elasticsearch 6

ES6 does not register `bulk.item.errors.dlq` and its bulk client never populates
`BulkItemError.position`, so the mode stays pinned to `fail`.
