package com.datamountaineer.streamreactor.connect.cassandra

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._

/**
  * <h1>CassandraSinkTask</h1>
  *
  * Kafka Connect Cassandra sink task. Called by framework to put records to the
  * target sink
  * */
class CassandraSinkTask extends SinkTask with StrictLogging {
  private var writer : Option[CassandraJsonWriter] = None

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    CassandraSinkConfig.config.parse(props)
    val sinkConfig = new CassandraSinkConfig(props)
    logger.info("""
               |    ____        __        __  ___                  __        _
               |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
               |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
               | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
               |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
               |       ______                                __           _____ _       __
               |      / ____/___ _______________ _____  ____/ /________ _/ ___/(_)___  / /__
               |     / /   / __ `/ ___/ ___/ __ `/ __ \/ __  / ___/ __ `/\__ \/ / __ \/ //_/
               |    / /___/ /_/ (__  |__  ) /_/ / / / / /_/ / /  / /_/ /___/ / / / / / ,<
               |    \____/\__,_/____/____/\__,_/_/ /_/\__,_/_/   \__,_//____/_/_/ /_/_/|_|
               |
               | By Andrew Stevenson""".stripMargin)

    writer = Some(CassandraWriter(connectorConfig = sinkConfig, context = context))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    * */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.write(records.asScala.toList))
  }

  /**
    * Clean up Cassandra connections
    * */
  override def stop(): Unit = {
    logger.info("Stopping Cassandra sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) = {
    while (writer.get.insertCount.get > 0) {
      logger.info("Waiting for writes to flush.")
      Thread.sleep(10)
    }
  }
  override def version(): String = ""
}
