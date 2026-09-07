/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.elastic.common.writer

/**
 * Marker trait for writer-stage failures (thrown by `JsonBulkWriter.processRecord` before a
 * record ever becomes a `BulkOp`) that are a property of exactly one record, and so are safe to
 * divert under `bulk.item.errors=drop`/`dlq`.
 *
 * `JsonBulkWriter.insert` only offers diversion to failures marked with this trait; every other
 * `Throwable` from `processRecord` propagates and fails the task, exactly as it did before
 * per-record diversion existed. This is deliberately an allow-list, not a deny-list: most
 * exceptions `processRecord` can throw are schema-, config-, or KCQL-level problems (a header
 * missing from every record on the topic, PK extraction failing because a field was renamed,
 * `require(pks.nonEmpty)` for a misconfigured UPSERT) that affect every record on the topic, not
 * just the one that happened to be first. Diverting those would silently discard or DLQ the
 * entire topic's traffic while the task reported itself healthy — the exact silent-data-loss
 * failure mode `BulkItemErrorClassifier.isDivertible` guards against on the cluster-response
 * side. See `JsonBulkWriter.insert`.
 *
 * Do NOT mix this into a new exception unless the condition that triggers it is truly a property
 * of the individual `SinkRecord` (its own payload/headers/tombstone-ness), never of the
 * connector's configuration, the topic's schema, or KCQL.
 */
trait PerRecordWriterFailure { self: Throwable => }

/**
 * Thrown by `JsonBulkWriter.handleTombstone` when a tombstone (null value) arrives under
 * `behavior.on.null.values=FAIL`. Whether a given `SinkRecord` is a tombstone is a property of
 * that record alone — a topic can freely mix tombstones and non-tombstones — so this is safe to
 * divert independently of any other record in the batch.
 *
 * Extends `IllegalStateException` (rather than only mixing in the marker trait) to preserve the
 * pre-existing exception type for callers/tests that still expect `IllegalStateException` when
 * diversion is inactive.
 */
final class TombstoneRejectedException(message: String)
    extends IllegalStateException(message)
    with PerRecordWriterFailure
