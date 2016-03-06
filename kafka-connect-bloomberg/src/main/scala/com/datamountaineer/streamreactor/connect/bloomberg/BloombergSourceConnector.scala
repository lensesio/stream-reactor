package com.datamountaineer.streamreactor.connect.bloomberg

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._


/**
  * <h1>BloombergSourceConnector</h1>
  * Kafka connect Bloomberg Source connector
  *
  * Sets up BloombergSourceTask and configurations for the tasks.
  */
class BloombergSourceConnector extends SourceConnector with StrictLogging {
  //var configProps: util.Map[String, String]

  var bloombergSettings: BloombergSettings = null

  /**
    * Defines the sink class to use
    *
    * @return
    */
  override def taskClass(): Class[_ <: Task] = classOf[BloombergSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    //ConnectorUtils.groupPartitions()
    logger.info(s"Setting task configurations for $maxTasks workers.")

    val partitions = Math.min(bloombergSettings.subscriptions.size, maxTasks)
    bloombergSettings.subscriptions.grouped(partitions)
      .map(p => bloombergSettings.copy(subscriptions = p).asMap())
      .toList.asJava
  }

  override def stop(): Unit = {
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    **/
  override def start(props: util.Map[String, String]): Unit = {
    try {
      bloombergSettings = BloombergSettings(new ConnectorConfig(props))
    }
    catch {
      case t: Throwable => throw new ConnectException("Cannot start the Bloomberg connector due to invalid configuration.", t)
    }

  }

  /**
    * Gets the version of this sink
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion
}

