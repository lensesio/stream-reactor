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

object PulsarMessageFactory {
  def apply(settings: PulsarSinkSettings): PulsarMessages = {
    new PulsarMessages(settings)
  }
}

class PulsarWriter(settings: PulsarSinkSettings) extends StrictLogging with ErrorHandler {
  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)

  private val producersMap = scala.collection.mutable.Map.empty[String, Producer]
  val msgFactory = PulsarMessageFactory(settings)

  //TODO move to a connection factory, add TLS, batching options.
  private val conf = new ProducerConfiguration
  conf.setCompressionType(CompressionType.LZ4)
  val client = PulsarClient.create(settings.connection)

  def write(records: Iterable[SinkRecord]) = {
    val messages = msgFactory.create(records)

    val t = Try{
      messages.foreach{
        case (topic, message) => {
          val producer = producersMap.getOrElseUpdate(topic, client.createProducer(topic, conf))
          producer.send(message)
        }
      }
    }

    handleTry(t)
  }

  def flush = {}

  def close = {
    logger.info("Closing client")
    client.close()
  }
}

class PulsarMessages(settings: PulsarSinkSettings) extends StrictLogging with ErrorHandler {

  private val mappings: Map[String, Set[Kcql]] = settings.kcql.groupBy(k => k.getSource)

  def create(records: Iterable[SinkRecord]): Iterable[(String, Message)] = {
    records.flatMap{ record =>
      val topic = record.topic()
      //get the kcql statements for this topic
      val kcqls = mappings(topic)
      kcqls.map { k =>
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

        (pulsarTopic, MessageBuilder.create.setContent(json.getBytes).build)
      }
    }
  }
}
