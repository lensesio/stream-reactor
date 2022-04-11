package com.landoop.streamreactor.connect.hive.source

import com.datamountaineer.streamreactor.common.utils.JarManifest

import java.util
import com.landoop.streamreactor.connect.hive.sink.config.HiveSinkConfigDef
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.jdk.CollectionConverters.SeqHasAsJava


class HiveSourceConnector extends SourceConnector with StrictLogging {

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var props: util.Map[String, String] = _

  override def version(): String = manifest.version()
  override def taskClass(): Class[_ <: Task] = classOf[HiveSourceTask]
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
