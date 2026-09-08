# Elasticsearch / OpenSearch: per-record diversion for permanent bulk item errors

This document describes the behaviour of the `bulk.item.errors` setting shared by the
Elasticsearch (6/7) and OpenSearch sink connectors. The connector config reference keeps only a
short summary; the full semantics live here.

## What it does

Controls what happens to a bulk item that fails with a **permanent, per-document error**
(HTTP 400 — e.g. a mapper conflict).

| Value            | Behaviour                                                                                                                                                             |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fail` (default) | The connector throws. Kafka Connect kills the task with offsets uncommitted, so the whole batch is redelivered on restart. No data is lost, but the task stops until restarted. |
| `drop`           | The failing record(s) are **discarded** (logged at WARN) and the batch is treated as handled so offsets advance past them, keeping the task running. **No dead-letter queue is required.** |
| `dlq`            | The failing record(s) are reported to the dead-letter queue and the batch is treated as handled so offsets advance past them, keeping the task running.                |

`drop` and `dlq` both mean *"a poison document does not kill the task"* — **not** *"the task can
never fail"*. The difference between them is purely where the diverted record ends up:

- `drop` needs nothing beyond the connector itself, but the diverted record is **gone** — its only
  trace is the log. Drops are summarised in one WARN per batch (record coordinates plus the
  cluster's rejection reason for the first 10 records, then a count of the rest); enable DEBUG on
  `io.lenses.streamreactor.connect.elastic.common.dlq.ErrantRecordHandler` to log every dropped
  record individually.
- `dlq` needs a Kafka Connect dead-letter queue configured, but the diverted record is **durable**
  — it lands in the DLQ topic, consumable and replayable.

Use `drop` when you'd rather not stand up a DLQ topic and simply want poison records to stop
blocking the task. Use `dlq` when you need a durable, inspectable record of what was discarded.

## When does `dlq` actually take effect

`dlq` is ignored (and the connector behaves as `fail`) unless **all** of the following hold:

- **`errors.deadletterqueue.topic.name` is set** to a non-empty topic name. A non-null
  `ErrantRecordReporter` is not sufficient — Kafka Connect hands one out whenever a DLQ topic is
  set *or* `errors.log.enable=true`.
- **`errors.tolerance=all`.** Under `errors.tolerance=none`, Kafka Connect throws out of the
  report call once the record has been handed to the DLQ producer, so `dlq` cannot keep the task
  running (the record still reaches the DLQ, but the task then fails fast).
- **`bulk.strict.item.errors=true`** (the default). In tolerant mode, per-item bulk failures are
  never surfaced, so there is nothing to report.

`drop` has none of these preconditions — it needs no DLQ topic, no `errors.tolerance` setting, and
no `ErrantRecordReporter` at all. The only shared precondition is `bulk.strict.item.errors=true`
(without it, no per-item bulk failure is ever surfaced to the writer, so there is nothing to divert
either way).

Misconfigurations here produce a WARN at task `start()` rather than a silent no-op discovered
later — including a dedicated WARN whenever `drop` is set at all, since it is a deliberate,
unconditional data-loss trade-off that operators should be reminded of every time the task starts.

## Only per-document rejections are diverted

Both `drop` and `dlq` divert **only per-document rejections (HTTP 400**, e.g. a mapper conflict).
Batch-wide conditions still fail the task regardless of mode, because they are not caused by the
content of any single record — diverting them would discard good records while the task kept
running and reported itself healthy. These include:

- write-blocked index (403, `cluster_block_exception`)
- authorization failure (403, `security_exception`)
- missing index (404, `index_not_found_exception`)
- closed index (400, `index_closed_exception`)
- missing ingest pipeline (400, `illegal_argument_exception` — "pipeline with id [x] does not exist")

Status (not error type) is the primary check, so a renamed/new document-level error type stays
divertible without a connector release. The last two above are HTTP 400 but nonetheless
batch-wide, so they are excluded explicitly: `index_closed_exception` by error type, and the
missing-pipeline case by its reason, matched as a PREFIX after confirming the error type is
`illegal_argument_exception` (a plain substring match is satisfiable by a crafted document, since
a mapper error's `reason` embeds a preview of the rejected document verbatim). Note these are
excluded per item, so even a response where *every* item carries one of these batch-wide 400s
fails the task rather than diverting the whole batch.

### Two more things that are never divertible, regardless of mode

- **Structural violations the connector detects itself** before a request is ever sent — an
  empty index/id, or an id over the cluster's 512-byte hard limit. These are reported as ordinary
  `BulkItemError`s (HTTP 400, error type `connector_op_validation_exception`) so they go through
  the same strict/tolerant machinery as a genuine cluster rejection, but they are systemic (every
  record hitting the same KCQL/PK misconfiguration fails identically) rather than a one-off
  cluster rejection, so `isDivertible` excludes them explicitly. They also always surface even
  under `bulk.strict.item.errors=false` — unlike genuine cluster item errors, tolerant mode never
  swallows them, because doing so would silently discard every record on the topic rather than
  logging a rejected document at WARN.
- **Writer-stage failures that are not a property of the individual record.** A failure before a
  record ever becomes a bulk operation (PK extraction failing, an unsupported write mode, a
  `CreateIndex` resolution failure) is almost always caused by a KCQL/schema/config problem
  affecting every record on the topic, not by the content of the one record that happened to hit
  it first. Only one such failure is genuinely per-record and therefore divertible: a tombstone
  arriving under `behavior.on.null.values=FAIL` (whether a given record is a tombstone is a
  property of that record alone). Everything else always fails the task, in every mode.

## "If a batch of 100 records fails, do all 100 get diverted?"

**No.** Diversion (drop or DLQ) only ever happens to the individual records the cluster rejected
with a per-document HTTP 400 — never a whole batch wholesale. For a 100-record batch:

- **All 100 fail with a transport-level error** (bulk call timed out / connection reset / cluster
  returned a 5xx for the whole request): **0 are diverted.** There is no per-document response
  telling us which records are bad, so diverting them would discard (or DLQ) 100 perfectly-good
  documents. RETRY re-sends all 100; NOOP/THROW fail-fast.
- **Batch-wide permanent error** (e.g. 403 write-blocked, 404 missing index): **0 are diverted.**
  The whole batch is thrown and redelivered — nothing is diverted.
- **3 rejected with HTTP 400, rest fine** (`drop` or `dlq`): **only those 3 are diverted**
  (dropped or DLQ'd), are removed from the batch, and the other 97 are written normally.
- **3 rejected with 400 + some 429 (retriable) in the same response**: **0 are diverted.**
  Diversion is deferred whenever a retriable error is present anywhere in the same response —
  see "At-least-once semantics and mixed batches" below — so the full error set throws and the
  batch is redelivered with nothing diverted on this attempt. Under `error.policy=RETRY` this
  redelivery happens in-process (via `RetriableException`), without killing the task, as long as
  retries remain; under `THROW`/`NOOP` the task fails and the batch is redelivered only on
  restart.

The only way a large number of records is diverted is if the cluster genuinely rejected each of
them individually with a 400 — the intended "poison record" case. A single infrastructure blip or
a batch-wide misconfiguration never floods a DLQ topic or discards a whole batch via `drop`.

## At-least-once semantics and mixed batches

Diversion (both `drop` and `dlq`) is at-least-once, but scoped to a whole redelivered `put()`, not
to a retriable sub-attempt within one. Concretely: **a bulk response mixing a divertible
permanent error with a retriable one (e.g. a 400 alongside a 429) diverts nothing at all** — the
full error set throws for the whole batch, exactly like `fail` mode, and the batch is redelivered.

This is deliberate, not a missed case. The alternative — diverting the permanent subset
immediately and throwing only for the retriable remainder — would mean the same permanent
record(s) get diverted again on *every* redelivery attempt while the retriable condition
persists: N redeliveries produce N DLQ reports (or N drop log lines) for the same record, an
unbounded amplification bounded only by the connector's retry budget. Deferring means a mixed
batch diverts **at most once** — on whichever attempt eventually comes back with only divertible
errors (the retriable condition having cleared).

This deferred throw is itself retriable (`RetriableIntegrityException`, not a plain fatal
exception) precisely *because* every non-retriable error in the mixed response has already been
confirmed divertible: once the retriable remainder clears, a redelivery is guaranteed to make
progress. That distinction matters concretely under `error.policy=RETRY`: the batch is redelivered
in-process, via `RetriableException`, while retries remain — the task does **not** die on the
first mixed response. Under `error.policy=THROW`/`NOOP` the distinction is moot, since both
policies already fail fast on any fatal exception subtype regardless; there, only the batch's
redelivery on task restart (not an in-process retry) can get it back with the retriable condition
cleared. In the pathological case where the retriable condition never clears, the task fails once
its retry budget is exhausted having diverted nothing, which is the same outcome `fail` mode
always had for that batch.

The dedup that prevents double-diverting within one attempt is scoped to a single `put()` (a fresh
tracker is created per `write()`), keyed by `(topic, partition, offset, target)`. It deliberately
does **not** survive across a redelivered `put()` — so at-least-once here means "at most once per
distinct whole-`put()` redelivery", not "once per retriable sub-attempt".

## `bulk.item.errors.dlq.timeout.ms`

Bounds, per `dlq` report call, how long to wait for Kafka Connect's DLQ producer to acknowledge a
diverted record before giving up and failing the batch instead (the batch is then redelivered).
Default 30000 (30 seconds). Not registered on ES6 (`dlq` can never take effect there).

The bound applies **once per group of records reported together** — e.g. all the permanent errors
resolved from a single bulk response — not once per individual record: N records reported in the
same call share one deadline rather than each getting the full timeout in sequence, so a single
`dlq` report call cannot block for longer than this value regardless of how many records it
covers. Keep it well below `max.poll.interval.ms`: a `bulk.item.errors=dlq` batch can still block
for up to this long once per `put()`.

## Fan-out to multiple KCQL targets

A record that fans out to more than one KCQL target may be committed successfully to one index and
diverted for another; anything consuming diverted records (the WARN logs for `drop`, or the DLQ
topic for `dlq`) should expect to see records that were also partially written.

This also affects the `connect.progress.enabled` counter: it counts by Kafka coordinates
(`topic`/`partition`/`offset`), not by KCQL target, so a record diverted at even ONE of its
targets is excluded from the delivered-record count entirely — even though it was genuinely
written at every OTHER target it fanned out to. The counter is a coarse liveness signal, not a
per-target delivery audit; use the WARN logs (`drop`) or the DLQ topic (`dlq`) for that.

## Elasticsearch 6

ES6 does not register `bulk.item.errors` and its bulk client never populates
`BulkItemError.position`, so the mode stays pinned to `fail`.

## Upgrade note: `error.policy=NOOP` behaviour change

This feature closes a pre-existing silent-data-loss path shared by all three connectors
(ES6, ES7, OpenSearch), which is a behavioural change independent of whether you use
`bulk.item.errors` at all:

- **Before:** under `error.policy=NOOP`, a bulk transport failure (connection reset, timeout —
  no per-item response received at all) or a permanent per-item bulk error (under
  `bulk.strict.item.errors=true`) was logged at WARN and swallowed. The task kept running and
  Kafka offsets advanced past records whose write outcome was unknown or known-failed.
- **After:** both now fail the task (transport failures via `RetriableIntegrityException`,
  permanent item errors via `FatalConnectException`) instead of being swallowed. `error.policy
  =RETRY` still retries a transport failure while budget remains; `error.policy=THROW` was
  already unaffected.

If you run `error.policy=NOOP` today specifically to keep the task alive through occasional
poison records, use `bulk.item.errors=drop` (no DLQ needed) or `bulk.item.errors=dlq` instead —
those are scoped to genuine per-document rejections only, unlike NOOP's previous blanket
swallow-everything behaviour, which also silently ate transport-level failures that had nothing
to do with any specific record.
