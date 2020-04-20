package io.lenses.streamreactor.connect.aws.s3.sink

import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import io.lenses.streamreactor.connect.aws.s3.config.S3SinkConfigDef
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.Logger

import scala.collection.JavaConverters._

class S3SinkConnector extends SinkConnector {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var props: util.Map[String, String] = _

  override def version(): String = manifest.version()

  override def taskClass(): Class[_ <: Task] = classOf[S3SinkTask]

  override def config(): ConfigDef = S3SinkConfigDef.config

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Creating S3 sink connector")
    this.props = props
  }

  override def stop(): Unit = ()

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Creating $maxTasks tasks config")
    List.fill(maxTasks)(props).asJava
  }
}