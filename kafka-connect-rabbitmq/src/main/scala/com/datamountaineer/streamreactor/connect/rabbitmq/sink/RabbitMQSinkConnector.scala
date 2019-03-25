package com.datamountaineer.streamreactor.connect.rabbitmq.sink

import java.util

import com.datamountaineer.streamreactor.connect.rabbitmq.config.{RabbitMQConfig, RabbitMQConfigConstants}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.util.ConnectorUtils
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class RabbitMQSinkConnector extends SinkConnector with StrictLogging {
    private var configProps: util.Map[String,String] = _
    private val configDef: ConfigDef = RabbitMQConfig.config
    private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

    override def start(props: util.Map[String, String]): Unit = configProps = props

    override def taskClass(): Class[_ <: Task] = classOf[RabbitMQSinkTask]

    override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
        logger.info(s"Setting task configurations for $maxTasks workers.")
        (1 to maxTasks).map(_ => configProps).toList.asJava
    }

    override def stop(): Unit = {}

    override def config(): ConfigDef = configDef

    override def version(): String = manifest.version()
}
