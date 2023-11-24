/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.pulsar.sink

import io.lenses.kcql.Field
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.converters.FieldConverter
import io.lenses.streamreactor.common.converters.ToJsonWithProjections
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.connect.pulsar.config.PulsarSinkSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.ListHasAsScala

case class PulsarMessageTemplateBuilder(settings: PulsarSinkSettings) extends StrictLogging with ErrorHandler {

  private val mappings: Map[String, Set[Kcql]] = settings.kcql.groupBy(k => k.getSource)

  @nowarn
  def extractMessageKey(record: SinkRecord, k: Kcql): Option[String] =
    if (k.getWithKeys != null && k.getWithKeys().size() > 0) {
      val parentFields = null

      // Get the fields to construct the key for pulsar
      val (partitionBy, schema, value) = if (k.getWithKeys != null && k.getWithKeys().size() > 0) {
        (k.getWithKeys.asScala.map(f => Field.from(f, f, parentFields)),
         if (record.key() != null) record.keySchema() else record.valueSchema(),
         if (record.key() != null) record.key() else record.value(),
        )
      } else {
        (Seq(Field.from("*", "*", parentFields)),
         if (record.key() != null) record.keySchema() else record.valueSchema(),
         if (record.key() != null) record.key() else record.value(),
        )
      }

      val keyFields = partitionBy.map(FieldConverter.apply)

      val jsonKey = ToJsonWithProjections(
        keyFields.toSeq,
        schema,
        value,
        k.hasRetainStructure,
      )

      Some(jsonKey.toString)
    } else {
      None
    }

  @nowarn
  def create(records: Iterable[SinkRecord]): Iterable[MessageTemplate] =
    // in KCQL
    records.flatMap { record =>
      getKcqlStatementsForTopic(record).map {
        k =>
          val pulsarTopic = k.getTarget

          //optimise this via a map
          val fields        = k.getFields.asScala.map(FieldConverter.apply)
          val ignoredFields = k.getIgnoredFields.asScala.map(FieldConverter.apply)
          //for all the records in the group transform

          val json = ToJsonWithProjections(
            fields.toSeq,
            record.valueSchema(),
            record.value(),
            k.hasRetainStructure,
          )

          val recordTime =
            if (record.timestamp() != null) record.timestamp().longValue() else System.currentTimeMillis()
          val msgValue: Array[Byte]    = json.toString.getBytes
          val msgKey:   Option[String] = extractMessageKey(record, k)

          MessageTemplate(pulsarTopic, msgKey, msgValue, recordTime)
      }
    }

  private def getKcqlStatementsForTopic(record: SinkRecord) = {
    val topic = record.topic()
    //get the kcql statements for this topic
    val kcqls = mappings(topic)
    kcqls
  }
}
