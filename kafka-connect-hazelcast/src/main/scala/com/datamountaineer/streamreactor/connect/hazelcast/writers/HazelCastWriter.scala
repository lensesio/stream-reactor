/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.hazelcast.writers

import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkSettings, HazelCastStoreAsType, TargetType}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */

object HazelCastWriter {
  def apply(settings: HazelCastSinkSettings): HazelCastWriter = {
    new HazelCastWriter(settings)
  }
}

class HazelCastWriter(settings: HazelCastSinkSettings) extends StrictLogging
  with ConverterUtil with ErrorHandler {
  logger.info("Initialising Hazelcast writer.")

  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)
  val writers = getWriters(settings.topicObject)

  def getWriters(tp: Map[String, HazelCastStoreAsType]): Map[String, Writer] = {
    tp.map({
      case (t, o) =>
        o.targetType match {
          case TargetType.RELIABLE_TOPIC => (t, ReliableTopicWriter(settings.client, t, settings))
          case TargetType.RING_BUFFER => (t, RingBufferWriter(settings.client, t, settings))
          case TargetType.QUEUE => (t, QueueWriter(settings.client, t, settings))
          case TargetType.SET => (t, SetWriter(settings.client, t, settings))
          case TargetType.LIST => (t, ListWriter(settings.client, t, settings))
          case TargetType.IMAP => (t, MapWriter(settings.client, t, settings))
          case TargetType.MULTI_MAP => (t, MultiMapWriter(settings.client, t, settings))
          case TargetType.ICACHE => (t, ICacheWriter(settings.client, t, settings))
        }
    })
  }

  /**
    * Write records to Hazelcast
    *
    * @param records The sink records to write.
    **/
  def write(records: Set[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped = records.map(r => (r.topic, r))
      grouped.foreach({
        case (topic, payload) =>
          val writer = writers.get(topic)
          val t = Try(writer.foreach(w => w.write(payload)))
          handleTry(t)
      })
      logger.debug(s"Written ${records.size}")
    }
  }

  def close(): Unit = {
    logger.info("Shutting down Hazelcast client.")
    writers.foreach({case (_, w) => w.close})
    settings.client.shutdown()
  }

  def flush(): Unit = {}
}
