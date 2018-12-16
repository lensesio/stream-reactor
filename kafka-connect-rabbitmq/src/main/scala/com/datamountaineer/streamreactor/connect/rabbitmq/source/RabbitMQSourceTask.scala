package com.datamountaineer.streamreactor.connect.rabbitmq.source

import java.util
import java.util.Collections

import com.datamountaineer.streamreactor.connect.rabbitmq.client.{RabbitMQClient, RabbitMQConsumer}
import com.datamountaineer.streamreactor.connect.rabbitmq.config.{RabbitMQConfig, RabbitMQSettings}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

class RabbitMQSourceTask extends SourceTask with StrictLogging {
    private var consumer : RabbitMQConsumer = _
    private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

    override def start(props: util.Map[String, String]): Unit = {
        logger.info(manifest.printManifest())
        val rabbitMQConfig = RabbitMQConfig(props)
        val rabbitMQSettings = RabbitMQSettings(rabbitMQConfig)
        consumer = RabbitMQConsumer(rabbitMQSettings)
        consumer.start()
    }

    override def poll(): util.List[SourceRecord] = {
        consumer.getRecords().orNull
    }

    override def stop(): Unit = {
        logger.info("Stopping RabbitMQClient source.")
        consumer.stop()
    }

    override def version(): String = manifest.version()
}
