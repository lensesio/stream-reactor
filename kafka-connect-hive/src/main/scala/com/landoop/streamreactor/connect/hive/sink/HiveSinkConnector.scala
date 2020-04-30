package com.landoop.streamreactor.connect.hive.sink

import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.landoop.streamreactor.connect.hive.sink.config.HiveSinkConfigDef
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConverters._

class HiveSinkConnector extends SinkConnector {

  val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var props: util.Map[String, String] = _

  override def version(): String = manifest.version()
  override def taskClass(): Class[_ <: Task] = classOf[HiveSinkTask]
  override def config(): ConfigDef = HiveSinkConfigDef.config

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Creating hive sink connector")
    this.props = props
  }

  override def stop(): Unit = ()

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Creating $maxTasks tasks config")
    List.fill(maxTasks)(props).asJava
  }
}