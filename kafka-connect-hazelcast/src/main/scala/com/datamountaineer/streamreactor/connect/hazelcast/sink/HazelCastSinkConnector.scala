package com.datamountaineer.streamreactor.connect.hazelcast.sink

import java.util

import com.datamountaineer.streamreactor.connect.hazelcast.config.HazelCastSinkConfig
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
class HazelCastSinkConnector extends SinkConnector with StrictLogging {
  private var configProps : Option[util.Map[String, String]] = None
  private val configDef = HazelCastSinkConfig.config

  /**
    * States which SinkTask class to use
    * */
  override def taskClass(): Class[_ <: Task] = classOf[HazelCastSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    * */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(c => configProps.get).toList
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    * */
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Hazelcast sink task.")
    configProps = Some(props)
  }

  override def stop(): Unit = {}
  override def version(): String = getClass.getPackage.getImplementationVersion
  override def config(): ConfigDef = configDef
}

