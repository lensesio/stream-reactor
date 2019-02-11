package com.datamountaineer.streamreactor.connect.rabbitmq.source

import java.util

import com.datamountaineer.streamreactor.connect.rabbitmq.client.RabbitMQConsumer
import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

class RabbitMQSourceTask extends SourceTask with StrictLogging {
    private var consumer : RabbitMQConsumer = _
    private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

    override def start(props: util.Map[String, String]): Unit = {
        logger.info(manifest.printManifest())
        consumer = initializeConsumer(props)
        consumer.start()
    }

    override def poll(): util.List[SourceRecord] = {
        consumer.getRecords().orNull
    }

    override def stop(): Unit = {
        consumer.stop()
    }

    override def version(): String = manifest.version()

    protected def initializeConsumer(props: util.Map[String,String]): RabbitMQConsumer = {
        val rabbitMQSettings = RabbitMQSettings(props)
        RabbitMQConsumer(rabbitMQSettings)
    }
}
