package com.datamountaineer.streamreactor.connect.cassandra

import java.util

import com.datamountaineer.streamreactor.connect.Logging
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
class CassandraSinkTask extends SinkTask with Logging {
  private var writer : Option[CassandraJsonWriter] = None

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    CassandraSinkConfig.config.parse(props)
    val sinkConfig = new CassandraSinkConfig(props)
    log.info("Setting up Cassandra writer.")
    writer = Some(CassandraWriter(connectorConfig = sinkConfig, context = context))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    * */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.map(w=> w.write(records.asScala.toList))
  }

  /**
    * Clean up Cassandra connections
    * */
  override def stop(): Unit = {
    log.info("Stopping Cassandra sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) = {
    while (writer.get.insertCount.get > 0) {
      log.info("Waiting for writes to flush.")
      Thread.sleep(10)
    }
  }
  override def version(): String = ""
}
