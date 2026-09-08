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
 * record becomes a `BulkOp`) that are a property of exactly one record and safe to divert under
 * `bulk.item.errors=drop`/`dlq`. See `JsonBulkWriter.insert`.
 *
 * Allow-list, not a deny-list: only mix this in when the failure is a property of the individual
 * `SinkRecord` (payload/headers/tombstone-ness), never of connector config, topic schema, or
 * KCQL. Every other `Throwable` from `processRecord` propagates and fails the task, since those
 * causes (a header missing from every record, a renamed PK field, `require(pks.nonEmpty)` for a
 * misconfigured UPSERT) affect the whole topic, not one record.
 */
trait PerRecordWriterFailure { self: Throwable => }

/**
 * Thrown by `JsonBulkWriter.handleTombstone` for a tombstone under
 * `behavior.on.null.values=FAIL`. Tombstone-ness is a property of the individual record, so this
 * is divertible independently of any other record in the batch.
 *
 * Extends `IllegalStateException` (not just the marker trait) so callers/tests expecting
 * `IllegalStateException` when diversion is inactive are unaffected.
 */
final class TombstoneRejectedException(message: String)
    extends IllegalStateException(message)
    with PerRecordWriterFailure
