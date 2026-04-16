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
package io.lenses.streamreactor.connect.azure.cosmosdb.config

import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.sink.extractors.KafkaConnectExtractor
import org.apache.kafka.connect.sink.SinkRecord

trait KeySource {
  def generateId(sinkRecord: SinkRecord): Either[Throwable, AnyRef]
}

case object KeyKeySource extends KeySource {
  override def generateId(sinkRecord: SinkRecord): Either[Throwable, AnyRef] =
    KafkaConnectExtractor.extractFromKey(sinkRecord, Option.empty)
}

case object MetadataKeySource extends KeySource {
  override def generateId(sinkRecord: SinkRecord): Either[Throwable, AnyRef] =
    "%s-%d-%s".format(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset()).asRight
}

case class KeyPathKeySource(path: String) extends KeySource {
  override def generateId(sinkRecord: SinkRecord): Either[Throwable, AnyRef] =
    KafkaConnectExtractor.extractFromKey(sinkRecord, path.some)
}

case class ValuePathKeySource(path: String) extends KeySource {
  override def generateId(sinkRecord: SinkRecord): Either[Throwable, AnyRef] =
    KafkaConnectExtractor.extractFromValue(sinkRecord, path.some)
}
