package com.datamountaineer.streamreactor.connect.rabbitmq.source

import java.util
import java.util.Collections

import com.datamountaineer.streamreactor.connect.rabbitmq.client.{RabbitMQClient, RabbitMQConsumer}
import com.datamountaineer.streamreactor.connect.rabbitmq.config.{RabbitMQConfig, RabbitMQSettings}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

class RabbitMQSourceTask extends SourceTask with StrictLogging {
    private var consumer : Option[RabbitMQConsumer] = None
    private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

    override def start(props: util.Map[String, String]): Unit = {
        logger.info(manifest.printManifest())
        val rabbitMQConfig = RabbitMQConfig(props)
        val rabbitMQSettings = RabbitMQSettings(rabbitMQConfig)
        consumer = Option(new RabbitMQConsumer(rabbitMQSettings))
        consumer match {
            case Some(consumer) => consumer.start()
            case None => logger.info("RabbitMQ Client Failed to Start. Exiting...")
        }
    }

    override def poll(): util.List[SourceRecord] = {
        var records: util.List[SourceRecord] = consumer match {
            case Some(c) => c.getRecords()
            case None => null
        }

        records
    }

    override def stop(): Unit = {
        logger.info("Stopping RabbitMQClient source.")
//        consumer match {
//            case Some(consumer) => consumer.stop()
//        }
        consumer.map(c => c.stop())
    }

    override def version(): String = manifest.version()
}
