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

package com.datamountaineer.streamreactor.connect.druid

import java.util
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.sink.{ SinkTask, SinkRecord }
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.TopicPartition
import com.datamountaineer.streamreactor.connect.druid.writer.DruidDbWriter
import com.datamountaineer.streamreactor.connect.druid.config._

/**
  * Created by andrew@datamountaineer.com on 04/03/16. 
  * stream-reactor
  */
class DruidSinkTask extends SinkTask with StrictLogging {
  var writer: Option[DruidDbWriter] = None

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      """
        |
        |    ____        __        __  ___                  __        _
        |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
        |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
        | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
        |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
        |       ____             _     _______ _       __
        |      / __ \_______  __(_)___/ / ___/(_)___  / /__ By Stefan Bocutiu
        |     / / / / ___/ / / / / __  /\__ \/ / __ \/ //_/
        |    / /_/ / /  / /_/ / / /_/ /___/ / / / / / ,<
        |   /_____/_/   \__,_/_/\__,_//____/_/_/ /_/_/|_|
        |
        |
      """.stripMargin)

    DruidSinkConfig.config.parse(props)
    val sinkConfig = new DruidSinkConfig(props)
    val settings = DruidSinkSettings(sinkConfig)
    logger.info(
      s"""Settings:
          |$settings
      """.stripMargin)
    writer = Some(DruidDbWriter(settings))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    if (records.size() == 0) {
      logger.info("Empty list of records received.")
    }
    else {
      require(writer.nonEmpty, "Writer is not set!")
      writer.foreach(w => w.write(records.toSeq))
    }
  }

  /**
    * Clean up Cassandra connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Hbase sink.")
    //writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) : Unit = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
