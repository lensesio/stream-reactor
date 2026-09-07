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
package io.lenses.streamreactor.connect.elastic.common.dlq

import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import org.apache.kafka.connect.sink.SinkRecord

/**
 * Identifies a single (record, target) DLQ report within one `put()` call, used to avoid
 * reporting the same record twice as `JsonBulkWriter.insert` walks multiple batches / KCQLs.
 *
 * `target` is deliberately part of the key, not just the record: one `SinkRecord` can fan out
 * to several `BulkOp`s when more than one KCQL targets the same source topic, and a record
 * that is rejected by index A but written successfully to index B must still be reported for
 * A — the two outcomes are independent and both belong in the DLQ / success path respectively.
 *
 * Note this dedup is scoped to a single `put()` invocation (a fresh set is created in
 * `JsonBulkWriter.write`). It intentionally does NOT survive across a redelivered `put()` — if
 * Kafka Connect redelivers the same batch (e.g. after a `RetriableException` under
 * `error.policy=RETRY`), already-reported records may be reported again. DLQ delivery is
 * at-least-once, matching Kafka Connect's own DLQ semantics.
 */
final case class DlqKey(topic: String, partition: Int, offset: Long, target: String)

object DlqKey {
  def forOp(op: BulkOp, record: SinkRecord): DlqKey =
    DlqKey(record.topic(), record.kafkaPartition(), record.kafkaOffset(), op.index)

  def forTarget(record: SinkRecord, target: String): DlqKey =
    DlqKey(record.topic(), record.kafkaPartition(), record.kafkaOffset(), target)
}
