/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.pulsar.sink

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.{FieldConverter, Transform}
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarSinkSettings
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.pulsar.client.api._

import scala.collection.JavaConversions._
import scala.util.Try

object PulsarWriter {
  def apply(settings: PulsarSinkSettings): PulsarWriter = {
    new PulsarWriter(PulsarClient.create(settings.connection), settings)
  }
}

class PulsarWriter(client: PulsarClient, settings: PulsarSinkSettings) extends StrictLogging with ErrorHandler {

  private val conf = new ProducerConfiguration
  // Enable compression
  //TODO: pick it up from settings
  conf.setCompressionType(CompressionType.LZ4)
  private val producersMap = scala.collection.mutable.Map.empty[String, Producer]

  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)
  private val mappings: Map[String, Set[Kcql]] = settings.kcql.groupBy(k => k.getSource)

  def write(records: Iterable[SinkRecord]) = {

    val t = Try {
      records.foreach { record =>
        val topic = record.topic()
        //get the kcql statements for this topic
        val kcqls = mappings(topic)
        kcqls.foreach { k =>
          val pulsarTopic = k.getTarget
          //optimise this via a map
          val fields = k.getFields.map(FieldConverter.apply)
          val ignoredFields = k.getIgnoredFields.map(FieldConverter.apply)
          //for all the records in the group transform

          val json = Transform(
            fields,
            ignoredFields,
            record.valueSchema(),
            record.value(),
            k.hasRetainStructure
          )

          val producer = producersMap.getOrElseUpdate(pulsarTopic, client.createProducer(pulsarTopic, conf))
          val msg = MessageBuilder.create.setContent(json.getBytes).build

          // Send a message (waits until the message is persisted)
          producer.send(msg)
        }
      }
    }

    //handle any errors according to the policy.
    handleTry(t)
  }

  def flush = {}

  def close = {
    logger.info("Closing client")
    client.close()
  }
}
