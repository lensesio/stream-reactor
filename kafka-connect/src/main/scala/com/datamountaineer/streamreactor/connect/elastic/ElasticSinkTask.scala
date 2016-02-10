package com.datamountaineer.streamreactor.connect.elastic

import java.util

import com.datamountaineer.streamreactor.connect.Logging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import scala.collection.JavaConverters._

class ElasticSinkTask extends SinkTask with Logging {
  private var writer : Option[ElasticJsonWriter] = None

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    ElasticSinkConfig.config.parse(props)
    val sinkConig = new ElasticSinkConfig(props)
    writer = Some(ElasticWriter(config = sinkConig, context = context))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    * */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.map(w=>w.write(records.asScala.toList))
  }

  /**
    * Clean up writer
    * */
  override def stop(): Unit = {
    log.info("Stopping Elastic sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
  override def version(): String = ""
}
