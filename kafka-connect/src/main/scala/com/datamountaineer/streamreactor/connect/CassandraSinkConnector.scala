package com.datamountaineer.streamreactor.connect

import java.util

import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

/**
  * <h1>CassandraSinkConnector</h1>
  * Kafka connect Cassandra Sink connector
  *
  * Sets up CassandraSinkTask and configurations for the tasks.
  * */
class CassandraSinkConnector extends SinkConnector with Logging {
  //???
  private var configProps : util.Map[String, String] = null

  /**
    * States which SinkTask class to use
    * */
  override def taskClass(): Class[_ <: Task] = classOf[CassandraSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    * */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    log.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(c => configProps).toList.asJava
  }

  override def stop(): Unit = {}

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    * */
  override def start(props: util.Map[String, String]): Unit = {
    log.info(s"Starting Cassandra sink task with ${props.toString}.")
    configProps = props
    Try(new CassandraSinkConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start CassandraSink due to configuration error.", f)
      case _ =>
    }
  }

  //fix
  override def version(): String = ""
}
