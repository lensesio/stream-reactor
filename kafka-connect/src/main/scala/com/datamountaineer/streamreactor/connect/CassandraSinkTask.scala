package com.datamountaineer.streamreactor.connect

import java.util
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import scala.collection.JavaConverters._

/**
  * Created by andrew on 22/01/16.
  */

class CassandraSinkTask extends SinkTask with Logging {
  private var writer : Option[CassandraJsonWriter] = None

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    val sinkConfig = new CassandraSinkConfig(props)
    //check the options
    CassandraSinkConfig.config.parse(props)
    log.info("Setting up Cassandra writer.")
    writer = Some(CassandraWriter(connectorConfig = sinkConfig, context = context))
  }

  /**
    * Clean up Cassandra connections
    * */
  override def stop(): Unit = {
    log.info("Stopping Cassandra sink.")
    writer.foreach(w => w.close())
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    * */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    writer.get.write(records.asScala.toList)
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) = {}

  //fix1
  override def version(): String = ""
}
