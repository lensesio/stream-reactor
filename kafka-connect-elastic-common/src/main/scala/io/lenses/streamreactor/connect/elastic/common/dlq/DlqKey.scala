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
 * Identifies a (record, target) divert within one `put()` call — dedups as
 * `JsonBulkWriter.insert` walks multiple batches/KCQLs.
 *
 * `target` is part of the key: one `SinkRecord` can fan out to several `BulkOp`s (multiple
 * KCQLs on the same topic); a record rejected for index A but written to index B is
 * independent per target.
 *
 * Scoped to one `put()` (fresh set per `JsonBulkWriter.write`); does not survive a redelivered
 * `put()`. Divert delivery is at-least-once.
 */
final case class DlqKey(topic: String, partition: Int, offset: Long, target: String)

object DlqKey {
  def forOp(op: BulkOp, record: SinkRecord): DlqKey =
    DlqKey(record.topic(), record.kafkaPartition(), record.kafkaOffset(), op.index)

  def forTarget(record: SinkRecord, target: String): DlqKey =
    DlqKey(record.topic(), record.kafkaPartition(), record.kafkaOffset(), target)
}
