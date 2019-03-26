package com.datamountaineer.streamreactor.connect.rabbitmq.source

import java.util

import com.datamountaineer.streamreactor.connect.rabbitmq.config.{RabbitMQConfig, RabbitMQConfigConstants}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class RabbitMQSourceConnector extends SourceConnector with StrictLogging {
    private var configProps: util.Map[String,String] = _
    private val configDef: ConfigDef = RabbitMQConfig.config
    private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

    override def start(props: util.Map[String, String]): Unit = configProps = props

    override def stop(): Unit = {}

    override def taskClass(): Class[_ <: Task] = classOf[RabbitMQSourceTask]

    override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
        val raw = configProps.get(RabbitMQConfigConstants.KCQL_CONFIG)
        require(raw != null && !raw.isEmpty,  s"No ${RabbitMQConfigConstants.KCQL_CONFIG} provided!")

        val kcql = raw.split(";")
        val groups = ConnectorUtils.groupPartitions(kcql.toList.asJava, maxTasks)

        groups
            .filterNot(g => g.isEmpty)
            .map(g => {
                val taskConfigs = new java.util.HashMap[String,String]
                taskConfigs.putAll(configProps)
                taskConfigs.put(RabbitMQConfigConstants.KCQL_CONFIG, g.mkString(";"))
                taskConfigs.toMap.asJava
            })
    }

    override def config(): ConfigDef = configDef

    override def version(): String = manifest.version()
}
