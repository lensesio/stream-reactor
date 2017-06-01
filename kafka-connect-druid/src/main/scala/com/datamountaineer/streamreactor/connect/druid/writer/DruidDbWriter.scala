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

package com.datamountaineer.streamreactor.connect.druid.writer

import java.io.ByteArrayInputStream
import java.util

import com.datamountaineer.streamreactor.connect.druid.config.DruidSinkSettings
import com.datamountaineer.streamreactor.connect.sink.DbWriter
import com.github.nscala_time.time.Imports._
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.twitter.util.{Await, Future}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Responsible for writing the SinkRecord payload to Druid.
  */
class DruidDbWriter(settings: DruidSinkSettings) extends DbWriter with StrictLogging {
  private val tranquilityConfig =  TranquilityConfig.read(new ByteArrayInputStream(settings.tranquilityConfig.getBytes))
  private val senders: Map[String, Tranquilizer[util.Map[String, AnyRef]]] = settings.datasourceNames.map(
    { case(topic, ds) =>
      val dataSource = tranquilityConfig.getDataSource(ds)
      (topic, DruidBeams.fromConfig(dataSource).buildTranquilizer())
    })

  senders.foreach({case (_, sender) =>
    logger.info("Starting sender")
    sender.start
  })

  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("Empty sequence of records received...")
    } else {
      val converted = records.map { record =>
        require(record.value() != null && record.value().getClass == classOf[Struct], "The SinkRecord payload should be of type Struct")
        val extractor = settings.extractors(record.topic())
        val fieldsAndValues =  extractor.get(record.value.asInstanceOf[Struct]).toMap
        (record.topic(), fieldsAndValues)
      }.toMap

      val futures = senders.map({
        case (topic, sender) =>
          val records = converted
                          .filter({case (maptopic, _) => maptopic.equals(topic)})
                          .flatMap({case(_, fValues) => fValues})
          val map = records ++ Seq("timestamp"->DateTime.now.toString)
          sender.send(map)
      }).toList.asJava
      Await.result(Future.collect(futures))
    }
  }

  override def close(): Unit = {
    senders.foreach({ case (_, sender) =>
      sender.flush()
      sender.close()
    })
  }
}
