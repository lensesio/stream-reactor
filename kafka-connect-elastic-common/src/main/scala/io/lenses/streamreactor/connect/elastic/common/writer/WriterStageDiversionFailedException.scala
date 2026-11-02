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
 * Thrown by `JsonBulkWriter.write` when [[io.lenses.streamreactor.connect.elastic.common.dlq.ErrantRecordHandler.divertWriterStage]]
 * refuses to divert a batch of writer-stage failures (DLQ ack timeout, `report()` throwing under
 * `errors.tolerance=none`, or no DLQ available). Wraps the FIRST underlying extraction/conversion
 * failure as `getCause` so the real reason a record never became a `BulkOp` is not lost, but
 * names the actual failure (the diversion attempt) in its own message — without this, the task
 * would fail with something like "Header 'tenant_id' not found" when the real problem was a DLQ
 * ack timeout, sending an operator to debug the wrong thing.
 */
final class WriterStageDiversionFailedException(message: String, cause: Throwable)
    extends IllegalStateException(message, cause)
