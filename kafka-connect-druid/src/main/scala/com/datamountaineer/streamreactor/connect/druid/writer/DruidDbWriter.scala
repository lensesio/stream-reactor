/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.druid.writer

import java.io.ByteArrayInputStream
import com.datamountaineer.streamreactor.connect.schemas.StructFieldsExtractor
import com.datamountaineer.streamreactor.connect.druid.config.DruidSinkSettings
import com.datamountaineer.streamreactor.connect.sink.DbWriter
import com.metamx.tranquility.config.{PropertiesBasedConfig, TranquilityConfig}
import com.metamx.tranquility.druid.DruidBeams
import com.twitter.util.{Await, Future}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.joda.time.DateTime
import scala.collection.JavaConversions._

/**
  * Responsible for writing the SinkRecord payload to Druid.
  */
case class DruidDbWriter(dataSourceName: String,
                         config: TranquilityConfig[PropertiesBasedConfig],
                         fieldsExtractor: StructFieldsExtractor //,
                         //timeout: Duration = Duration.fromSeconds(600)
                        ) extends DbWriter with StrictLogging {
  private val dataSource = config.getDataSource(dataSourceName)
  private val sender = DruidBeams.fromConfig(dataSource).buildTranquilizer(dataSource.tranquilizerBuilder())

  sender.start()

  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.info("Empty sequence of records received...")
    } else {
      logger.info("asassssssssssss")
      val futures = records.flatMap { record =>
        require(record.value() != null && record.value().getClass == classOf[Struct], "The SinkRecord payload should be of type Struct")

        val fieldsAndValues = fieldsExtractor.get(record.value.asInstanceOf[Struct]).toMap

        if (fieldsAndValues.nonEmpty) {
          Some(fieldsAndValues)
        }
        else {
          None
        }
      }.map { map => sender.send(map + ("timestamp" -> DateTime.now.toString())) }

      Await.result(Future.collect(futures))
    }
  }

  override def close(): Unit = {
    sender.flush()
    sender.close()
  }
}


object DruidDbWriter {
  def apply(settings: DruidSinkSettings): DruidDbWriter = {
    DruidDbWriter(settings.datasourceName,
      TranquilityConfig.read(new ByteArrayInputStream(settings.tranquilityConfig.getBytes)),
      StructFieldsExtractor(settings.payloadFields.includeAllFields, settings.payloadFields.fieldsMappings))
  }
}
