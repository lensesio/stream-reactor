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

import org.apache.kafka.connect.sink.SinkRecord

/**
 * Kafka's own identity for a `SinkRecord` — `(topic, partition, offset)` — used instead of the
 * `SinkRecord` itself wherever only set-membership (was this record diverted?) is needed.
 *
 * `JsonBulkWriter.write` returns a `Set[RecordRef]` rather than `Set[SinkRecord]` for exactly
 * this reason: `SinkRecord.hashCode`/`equals` recurse through the entire payload `Struct` and
 * its `Schema`, and `AbstractElasticSinkTask.put` calls `contains` against this set once per
 * record in the batch, on every `put()`. `(topic, partition, offset)` alone is a cheap,
 * correct-enough key: Kafka guarantees it uniquely identifies a record.
 */
final case class RecordRef(topic: String, partition: Int, offset: Long)

object RecordRef {
  def of(record: SinkRecord): RecordRef = RecordRef(record.topic(), record.kafkaPartition(), record.kafkaOffset())
}
