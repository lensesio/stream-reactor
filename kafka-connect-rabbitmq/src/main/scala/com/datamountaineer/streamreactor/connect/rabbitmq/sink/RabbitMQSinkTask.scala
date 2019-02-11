package com.datamountaineer.streamreactor.connect.rabbitmq.sink

import java.util

import com.datamountaineer.streamreactor.connect.rabbitmq.client.RabbitMQProducer
import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._

class RabbitMQSinkTask extends SinkTask with StrictLogging {
    private var producer: RabbitMQProducer = _
    private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

    override def start(props: util.Map[String, String]): Unit = {
        logger.info(manifest.printManifest())
        producer = initializeProducer(props)
        producer.start()
    }

    override def put(records: util.Collection[SinkRecord]): Unit = {
//        producer.write(records.asScala.toList)
        val a = 1;
    }

    override def stop(): Unit = {
        producer.stop()
    }

    override def version(): String = manifest.version()

    protected def initializeProducer(props: util.Map[String,String]): RabbitMQProducer = {
        val rabbitMQSettings = RabbitMQSettings(props)
        RabbitMQProducer(rabbitMQSettings)
    }
}
