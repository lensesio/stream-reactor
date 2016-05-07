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

package com.datamountaineer.streamreactor.connect.redis.sink

import java.util
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisSinkConfig, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.redis.sink.writer.RedisDbWriter
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import scala.collection.JavaConversions._

/**
  * <h1>RedisSinkTask</h1>
  *
  * Kafka Connect Redis sink task. Called by framework to put records to the
  * target sink
  **/
class RedisSinkTask extends SinkTask with StrictLogging {
  var writer: Option[RedisDbWriter] = None

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(

      """
        | ____              __                                                 __
        |/\  _`\           /\ \__              /'\_/`\                        /\ \__           __
        |\ \ \/\ \     __  \ \ ,_\    __      /\      \    ___   __  __    ___\ \ ,_\    __   /\_\    ___      __     __   _ __
        | \ \ \ \ \  /'__`\ \ \ \/  /'__`\    \ \ \__\ \  / __`\/\ \/\ \ /' _ `\ \ \/  /'__`\ \/\ \ /' _ `\  /'__`\ /'__`\/\`'__\
        |  \ \ \_\ \/\ \L\.\_\ \ \_/\ \L\.\_   \ \ \_/\ \/\ \L\ \ \ \_\ \/\ \/\ \ \ \_/\ \L\.\_\ \ \/\ \/\ \/\  __//\  __/\ \ \/
        |   \ \____/\ \__/.\_\\ \__\ \__/.\_\   \ \_\\ \_\ \____/\ \____/\ \_\ \_\ \__\ \__/.\_\\ \_\ \_\ \_\ \____\ \____\\ \_\
        |    \/___/  \/__/\/_/ \/__/\/__/\/_/    \/_/ \/_/\/___/  \/___/  \/_/\/_/\/__/\/__/\/_/ \/_/\/_/\/_/\/____/\/____/ \/_/
        |
        |
        | ____               __                  ____                __
        |/\  _`\            /\ \  __            /\  _`\   __        /\ \
        |\ \ \L\ \     __   \_\ \/\_\    ____   \ \,\L\_\/\_\    ___\ \ \/'\ By Stefan Bocutiu
        | \ \ ,  /   /'__`\ /'_` \/\ \  /',__\   \/_\__ \\/\ \ /' _ `\ \ , <
        |  \ \ \\ \ /\  __//\ \L\ \ \ \/\__, `\    /\ \L\ \ \ \/\ \/\ \ \ \\`\
        |   \ \_\ \_\ \____\ \___,_\ \_\/\____/    \ `\____\ \_\ \_\ \_\ \_\ \_\
        |    \/_/\/ /\/____/\/__,_ /\/_/\/___/      \/_____/\/_/\/_/\/_/\/_/\/_/
      """.stripMargin)

    RedisSinkConfig.config.parse(props)
    val sinkConfig = new RedisSinkConfig(props)
    val settings = RedisSinkSettings(sinkConfig)
    logger.info(
      s"""Settings:
          |$settings
      """.stripMargin)
    writer = Some(RedisDbWriter(settings))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    if (records.isEmpty) {
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
    logger.info("Stopping Redis sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) : Unit = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
