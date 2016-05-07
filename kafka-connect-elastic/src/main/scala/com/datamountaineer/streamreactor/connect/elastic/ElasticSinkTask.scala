/**
  * Copyright 2015 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.elastic

import java.util
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import scala.collection.JavaConverters._

class ElasticSinkTask extends SinkTask with StrictLogging {
  private var writer : Option[ElasticJsonWriter] = None

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      """
        |
        |    ____        __        __  ___                  __        _
        |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
        |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
        | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
        |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
        |       ________           __  _      _____ _       __
        |      / ____/ /___ ______/ /_(_)____/ ___/(_)___  / /__
        |     / __/ / / __ `/ ___/ __/ / ___/\__ \/ / __ \/ //_/
        |    / /___/ / /_/ (__  ) /_/ / /__ ___/ / / / / / ,<
        |   /_____/_/\__,_/____/\__/_/\___//____/_/_/ /_/_/|_|
        |
        |
        |by Andrew Stevenson
      """.stripMargin)

    ElasticSinkConfig.config.parse(props)
    val sinkConfig = new ElasticSinkConfig(props)
    writer = Some(ElasticWriter(config = sinkConfig, context = context))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    * */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w=>w.write(records.asScala.toList))
  }

  /**
    * Clean up writer
    * */
  override def stop(): Unit = {
    logger.info("Stopping Elastic sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    logger.info("Flushing Elastic Sink")
  }
  override def version(): String = getClass.getPackage.getImplementationVersion
}
