package com.datamountaineeer.streamreactor.connect.blockchain.source

import java.util

import com.datamountaineeer.streamreactor.connect.blockchain.config.BlockchainConfig
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConversions._

class BlockchainSourceConnector extends SourceConnector with StrictLogging {
  private var configProps: Option[util.Map[String, String]] = None

  /**
    * Defines the source class to use
    *
    * @return
    */
  override def taskClass(): Class[_ <: Task] = classOf[BlockchainsourceTask]

  /**
    * Set the configuration for each work and determine the split.
    *
    * @param maxTasks The max number of task workers be can spawn.
    * @return a List of configuration properties per worker.
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = Seq(configProps.get)

  /**
    * Start the sink and set to configuration.
    *
    * @param props A map of properties for the connector and worker.
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Blockchain source task with ${props.toString}.")
    configProps = Some(props)
  }

  override def stop(): Unit = {}

  /**
    * Gets the version of this sink.
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion

  override def config(): ConfigDef = BlockchainConfig.config
}

