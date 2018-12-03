package com.datamountaineer.streamreactor.connect.rabbitmq.source

import java.util
import java.util.Collections

import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQConfig
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

class RabbitMQSourceConnector extends SourceConnector with StrictLogging {
    private var configProps: util.Map[String,String] = _
    private val configDef: ConfigDef = RabbitMQConfig.config
    private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

    override def start(props: util.Map[String, String]): Unit = configProps = props

    override def stop(): Unit = {}

    override def taskClass(): Class[_ <: Task] = classOf[RabbitMQSourceTask]

    override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
        Collections.singletonList(configProps)
    }

    override def config(): ConfigDef = configDef

    override def version(): String = manifest.version()
}
