package com.datamountaineer.streamreactor.connect.kudu

import java.util
import com.datamountaineer.streamreactor.connect.utils.Logging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
class KuduSinkTask extends SinkTask with Logging {
  private var writer : Option[KuduWriter] = None

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    KuduSinkConfig.config.parse(props)
    val sinkConfig = new KuduSinkConfig(props)
    writer = Some(KuduWriter(config = sinkConfig, context = context))
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
    log.info("Stopping Elastic sink.")
    writer.foreach(w => w.close())
  }

  //0.7 has
  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.map(w=>w.flush())
  }
  override def version(): String = ""
}
