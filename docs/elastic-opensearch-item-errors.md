# Elasticsearch / OpenSearch: per-record diversion for bulk item errors and writer-stage failures

This document describes the behaviour of the `bulk.item.errors` setting shared by the
Elasticsearch (6/7) and OpenSearch sink connectors. The connector config reference keeps only a
short summary; the full semantics live here.

## What it does

Controls what happens to a record that fails in either of two independent ways:

- a **permanent, per-document bulk error** (HTTP 400 — e.g. a mapper conflict) returned by the
  cluster once a document has actually been sent, or
- a **writer-stage failure** — the record never became a bulk operation at all, because
  extracting or converting a header/key/value used for its primary key or index target failed
  (a missing header, an unresolvable value path, a PK field whose value won't convert to the
  declared type, a malformed payload), or because it is a tombstone under
  `behavior.on.null.values=FAIL`.

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

## Only per-document BULK rejections are diverted

This section is about the BULK-response side specifically — see "Writer-stage failures" below
for the pre-bulk side, which has its own, much shorter, never-divertible list. Both `drop` and
`dlq` divert **only per-document rejections (HTTP 400**, e.g. a mapper conflict) from a bulk
response. Batch-wide conditions still fail the task regardless of mode, because they are not caused by the
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

### One more thing that is never divertible, regardless of mode

**Structural violations the connector detects itself** before a request is ever sent — an
empty index/id, or an id over the cluster's 512-byte hard limit. These are reported as ordinary
`BulkItemError`s (HTTP 400, error type `connector_op_validation_exception`) so they go through
the same strict/tolerant machinery as a genuine cluster rejection, but they are systemic (every
record hitting the same KCQL/PK misconfiguration fails identically) rather than a one-off
cluster rejection, so `isDivertible` excludes them explicitly. They also always surface even
under `bulk.strict.item.errors=false` — unlike genuine cluster item errors, tolerant mode never
swallows them, because doing so would silently discard every record on the topic rather than
logging a rejected document at WARN.

## Writer-stage failures — divertible by default, with a short construction-time exception list

A "writer-stage" failure happens before a record ever becomes a bulk operation: extracting or
converting the header/key/value used for a primary key or index target failed, the JSON/Struct
payload itself couldn't be parsed, or the record is a tombstone under
`behavior.on.null.values=FAIL`. All of these are properties of the individual record's content —
exactly like a bad Elasticsearch mapping rejecting one document with a per-item HTTP 400, which
has always been divertible — so **all of them are divertible under `drop`/`dlq`**, the same as
per-document bulk rejections:

- a missing, null, or non-string header used for a primary key or index target
- a value or key path that doesn't resolve in this record's payload (missing field, wrong shape,
  array where a primitive was expected, a null key when the PK path starts with `_key`)
- a primary key resolving to a JSON `null` or a missing field (previously silently written under
  the bogus id `"null"` — now diverted instead; see "Behaviour change" below)
- a primary key field whose runtime value won't convert to its declared logical type (e.g. a
  string where a `Date` is expected)
- a malformed JSON/Struct payload that fails to parse or transform
- a tombstone arriving under `behavior.on.null.values=FAIL`

Only a handful of failures are **not** properties of any individual record — they depend solely
on the KCQL/config and would fail identically for every record on the topic. These are validated
once, at connector `start()`, so the task fails immediately on a bad KCQL rather than discarding
the whole topic one record at a time under `drop`/`dlq`. They are never diverted, in every mode:

- a PK path with a nested header key (`_header.a.b` — headers only support single-level lookups)
- `UPSERT` with no primary key declared
- an unsupported KCQL write mode (only `INSERT`/`UPSERT` are supported)
- a KCQL target string that doesn't parse (e.g. `_header` with no path)
- an invalid `WITHINDEXSUFFIX` date/time pattern, or an unmatched `}` in it

Under `drop`, a KCQL that genuinely names the wrong header still shows up clearly: one summary
WARN per `put()` naming the discarded count (never one WARN per record — see "Diversion is
batched per `put()`" below). Under `dlq`, the same misconfiguration is visible as the DLQ topic
filling at full throughput while the target index stays empty. Neither is silent, just not fatal.

### Behaviour change: a PK resolving to JSON `null` used to be written under a bogus id

Before this, `PrimaryKeyExtractor` silently returned Scala `null` for a PK field that resolved to
a JSON `null` or a missing node, and `pks.mkString` then stringified that into the literal id
`"null"` (or `"a-null"` for a composite key) — indexing the document under a bogus, collision-
prone id indistinguishable from a genuine extraction failure, and overwriting whichever record
last had a null PK. It now throws instead, exactly like the equivalent condition on the `Struct`
(non-JSON) path already did (`"Field 'x' is null"`), making it a divertible writer-stage failure
like any other bad PK. If you were relying on the old silent-`"null"`-id behaviour, this is a
breaking change.

### Diversion is batched per `put()`, not per record or per sub-batch

Writer-stage failures collected across an ENTIRE `write()` call — every topic, KCQL, and
`batch.size` sub-batch in the `put()` — are diverted together, in exactly one call, after every
bulk call in that `put()` has already succeeded. This matters for two reasons now that writer-
stage diversion is no longer restricted to the rare `FAIL`-tombstone case:

- **`dlq`:** DLQ acknowledgement waits share ONE deadline (`bulk.item.errors.dlq.timeout.ms`)
  across every future issued in that one call — the same guarantee bulk-item diversion already
  has (see below). Diverting per record instead would mean a KCQL no record on the topic can
  satisfy pays up to one FULL timeout **per record** inside a single `put()` — for a batch of
  hundreds or thousands of records, easily exceeding `max.poll.interval.ms` and evicting the task
  from its consumer group.
- **`drop`:** one summary WARN per `put()`, capped at 10 records of detail with the remainder at
  DEBUG — never one WARN per record.

If diversion is refused (DLQ ack timeout, `report()` throwing under `errors.tolerance=none`, or
no DLQ available), the task fails with a `WriterStageDiversionFailedException` naming the refused
diversion itself, with the original extraction/conversion failure preserved as its cause — not
the raw extraction error rethrown directly, so an operator investigating a DLQ ack timeout isn't
misled into debugging a phantom KCQL problem instead.

Like per-document bulk errors, `error.policy` never sees a writer-stage failure or a refused
diversion: neither passes through the `error.policy` machinery, so `error.policy=RETRY` will not
retry a DLQ ack timeout and `error.policy=NOOP` will not swallow it — `bulk.item.errors=fail`
(the default) always kills the task for these, regardless of `error.policy`.

## "If a batch of 100 records fails, do all 100 get diverted?"

**Usually not — but a KCQL every record fails to satisfy is the one case where all of them can
be.** Diversion (drop or DLQ) normally only ever happens to the individual records that are
actually bad — never a whole batch wholesale purely because of a transport or cluster-side
condition. For a 100-record batch:

- **All 100 fail with a transport-level error** (bulk call timed out / connection reset / cluster
  returned a 5xx for the whole request): **0 are diverted.** There is no per-document response
  telling us which records are bad, so diverting them would discard (or DLQ) 100 perfectly-good
  documents. RETRY re-sends all 100; NOOP/THROW fail-fast.
- **Batch-wide permanent bulk error** (e.g. 403 write-blocked, 404 missing index): **0 are
  diverted.** The whole batch is thrown and redelivered — nothing is diverted.
- **3 rejected with HTTP 400, rest fine** (`drop` or `dlq`): **only those 3 are diverted**
  (dropped or DLQ'd), are removed from the batch, and the other 97 are written normally.
- **3 rejected with 400 + some 429 (retriable) in the same response**: **0 are diverted.**
  Diversion is deferred whenever a retriable error is present anywhere in the same response —
  see "At-least-once semantics and mixed batches" below — so the full error set throws and the
  batch is redelivered with nothing diverted on this attempt. Under `error.policy=RETRY` this
  redelivery happens in-process (via `RetriableException`), without killing the task, as long as
  retries remain; under `THROW`/`NOOP` the task fails and the batch is redelivered only on
  restart.
- **All 100 fail at the WRITER STAGE** (e.g. a KCQL PK references a header genuinely absent from
  every record on the topic — a misconfiguration, not 100 coincidentally-poison records): **all
  100 are diverted**, in one `divertWriterStage` call. This is deliberate, not a bug: unlike a
  transport/cluster-side condition, a writer-stage failure is resolved per record from that
  record's own content, so the connector genuinely knows each of the 100 is individually bad —
  it isn't guessing. See "Writer-stage failures" above for why this is diversion working as
  intended, and how it stays visible (one WARN under `drop`, an emptying DLQ-vs-index gap under
  `dlq`) rather than silent.

A single infrastructure blip or a batch-wide BULK-ITEM misconfiguration never floods a DLQ topic
or discards a whole batch via `drop` — that guarantee is unchanged. It's specifically a
KCQL/config problem that makes every record fail at the WRITER STAGE that can, correctly, divert
an entire batch.

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

### A known residual: a refused writer-stage diversion can duplicate an earlier bulk-item DLQ report

Bulk-item diversion happens per bulk call, inside the loop over `batch.size` sub-batches;
writer-stage diversion happens once, at the very end of `write()`, after every bulk call in the
`put()` has already succeeded. This means a single `put()` can, in order: divert (and durably
report) a bulk-item error from sub-batch 1, then fail to divert a writer-stage failure collected
from sub-batch 2 (a DLQ ack timeout, say). The whole `put()` then throws
`WriterStageDiversionFailedException`, offsets do not commit, and the redelivered `put()`
re-reports sub-batch 1's bulk-item record(s) to the DLQ a second time.

This is a narrower version of the amplification problem "Diversion is batched per `put()`" above
solves for writer-stage failures on their own — it is not new, and it is strictly better than
before this change (previously a refused writer-stage divert could strand every bulk-item report
in the same `put()`, not just those preceding the failure). Extending bulk-item diversion to also
defer to end-of-`put()` would close this gap entirely, but would change the well-tested
deferred-diversion behaviour in `handleBulkResult` above and is deliberately out of scope here.
In practice this means: under `dlq`, an occasional duplicate DLQ record for an otherwise-healthy
bulk-item rejection, only on the redelivery immediately following a writer-stage diversion
failure — never a lost record.

## `bulk.item.errors.dlq.timeout.ms`

Bounds, per `dlq` report call, how long to wait for Kafka Connect's DLQ producer to acknowledge a
diverted record before giving up and failing the batch instead (the batch is then redelivered).
Default 30000 (30 seconds). Not registered on ES6 (`dlq` can never take effect there).

The bound applies **once per group of records reported together** — e.g. all the permanent
bulk-item errors resolved from a single bulk response, OR all the writer-stage failures collected
across an entire `put()` — not once per individual record: N records reported in the
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
`BulkItemError.position`, so the mode stays pinned to `fail`. This covers writer-stage failures
too, not just per-document bulk rejections — ES6 shares `JsonBulkWriter` with ES7/OpenSearch, but
since its mode can never be anything other than `fail`, no failure of either kind is ever
diverted there; a bad header/PK still fails the task on the first record. The connector is
expected to be retired, so this is not planned to change. ES6 does still benefit from the
construction-time validation (a bad `WITHINDEXSUFFIX` pattern or `UPSERT` with no PK now fails at
`start()` instead of on the first record) and the JSON-null-PK fix (see "Behaviour change"
above), since both are unconditional improvements to `JsonBulkWriter` itself.

## Upgrade note: construction-time validation can now stop a previously-running connector

Five KCQL/config-only faults — listed under "Writer-stage failures" above (a nested header PK
path, `UPSERT` with no PK, an unsupported write mode, an unparseable KCQL target, or an invalid
`WITHINDEXSUFFIX` pattern) — are now validated once at `start()` instead of on the first record
that happens to exercise the KCQL. For any topic that receives records regularly this is a
strict improvement: the task fails immediately at startup instead of running seemingly fine until
the first record arrives.

It is a **breaking change** for one specific, sharp case: a multi-KCQL connector where one KCQL
targets a topic that is currently idle. For example:

```
UPSERT INTO a SELECT * FROM idle-topic;    -- no PK declared, never received a record
INSERT INTO b SELECT * FROM busy-topic
```

Before this change, `idle-topic`'s bad KCQL was silently never exercised and the connector ran
indefinitely on `busy-topic` alone. Now the connector will not start at all until the KCQL is
fixed, even though the fault would never have surfaced under the old behaviour. The same applies
to a bad `WITHINDEXSUFFIX` pattern or an unparseable target on any KCQL for an idle topic. This
affects **Elasticsearch 6 too** (see the ES6 section above), since it shares `JsonBulkWriter`'s
construction path with ES7/OpenSearch even though its `bulk.item.errors` mode is always `fail`.

If you run multiple KCQLs and are not certain all of them are individually valid, check
`start()`/task logs after upgrading before assuming a quiet topic means a quiet KCQL.

See also "Behaviour change: a PK resolving to JSON `null` used to be written under a bogus id"
above — a second, narrower breaking change from this same body of work, at the per-record rather
than per-KCQL level.

## Upgrade note: `error.policy=NOOP` behaviour change

This feature closes a pre-existing silent-data-loss path shared by all three connectors
(ES6, ES7, OpenSearch), which is a behavioural change independent of whether you use
`bulk.item.errors` at all:

- **Before:** under `error.policy=NOOP`, a bulk transport failure (connection reset, timeout —
  no per-item response received at all) was logged at WARN and swallowed. The task kept running
  and Kafka offsets advanced past a batch whose write outcome was unknown.
- **After:** a transport failure is now classified as `RetriableIntegrityException` and fails
  the task under NOOP instead of being swallowed. `error.policy=RETRY` still retries it while
  budget remains; `error.policy=THROW` was already unaffected.
- **Unchanged:** a per-item bulk error (under `bulk.strict.item.errors=true`) already failed the
  task under NOOP before this change — permanent errors via `FatalConnectException`, retriable
  (429/409) ones via `RetriableIntegrityException`, both of which NOOP rethrows.

If you want the task to keep running through occasional poison records, use
`bulk.item.errors=drop` (no DLQ needed) or `bulk.item.errors=dlq` — those are scoped to genuine
per-document rejections and writer-stage failures only. NOOP never kept the task alive through
per-item rejections; what it did silently swallow were transport-level failures that had nothing
to do with any specific record.
