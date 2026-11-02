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
 * Thrown by `JsonBulkWriter.handleTombstone` for a tombstone under
 * `behavior.on.null.values=FAIL`.
 *
 * Historically the one marker-trait-tagged "allow-listed" writer-stage failure — every other
 * `Throwable` from `processRecord` used to fail the task even under `bulk.item.errors=drop`/`dlq`
 * (see git history for `PerRecordWriterFailure`). That allow-list is gone: `JsonBulkWriter.insert`
 * now diverts any writer-stage failure whenever diversion is active (see
 * [[io.lenses.streamreactor.connect.elastic.common.KcqlValues]] and `JsonBulkWriter`'s
 * construction-time validation for what is resolved upfront instead, so it fails at `start()`
 * rather than being a runtime deny-list entry). This class is kept as a plain
 * `IllegalStateException` — not tagged with any marker — purely so existing callers/tests that
 * assert `IllegalStateException` when diversion is inactive (`bulk.item.errors=fail`) are
 * unaffected by the removal.
 */
final class TombstoneRejectedException(message: String) extends IllegalStateException(message)
