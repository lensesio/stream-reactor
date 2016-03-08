package com.datamountaineer.streamreactor.connect.druid

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

/**
  * Created by andrew@datamountaineer.com on 04/03/16. 
  * stream-reactor
  */
class DruidSinkTask extends SinkTask with StrictLogging {
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
        |      / __ \_______  __(_)___/ / ___/(_)___  / /__
        |     / / / / ___/ / / / / __  /\__ \/ / __ \/ //_/
        |    / /_/ / /  / /_/ / / /_/ /___/ / / / / / ,<
        |   /_____/_/   \__,_/_/\__,_//____/_/_/ /_/_/|_|
        |
        |
      """.stripMargin)
  }
  override def put(records: util.Collection[SinkRecord]): Unit = ???
  override def flush(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = ???
  override def stop(): Unit = ???
  override def version(): String = ""
}
