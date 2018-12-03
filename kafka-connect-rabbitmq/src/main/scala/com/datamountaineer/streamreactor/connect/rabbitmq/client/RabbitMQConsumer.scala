package com.datamountaineer.streamreactor.connect.rabbitmq.client

import java.util
import java.util.Collections
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.rabbitmq.client._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord

class RabbitMQConsumer(settings: RabbitMQSettings) extends RabbitMQClient(settings) with Consumer with StrictLogging {
    private val blockingQueue = new LinkedBlockingQueue[SourceRecord]()

    logger.info(s"Connecting consumer to ${settings.hostname}:${settings.port}")

    override def handleConsumeOk(consumerTag: String): Unit = {
//        _consumerTag = consumerTag
        logger.info(s"Consumer ${consumerTag} connected successfully")
    }

    override def handleCancelOk(consumerTag: String): Unit = {
        logger.info(s"Consumer ${consumerTag} disconnected successfully")
    }

    override def handleCancel(consumerTag: String): Unit = {
        logger.warn(s"Consumer $consumerTag disconnected")
    }

    override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit = {
        logger.warn(s"Consumer $consumerTag disconnected",sig)
    }

    override def handleRecoverOk(consumerTag: String): Unit = {
        logger.info(s"Basic recover ok for consumer $consumerTag")
    }

    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        var message = new String(body, "UTF-8")
        val sourceRecord = new SourceRecord(Collections.singletonMap("channel",settings.queue),Collections.singletonMap("offset",1),settings.kafkaTopic,
            null,null,null,Schema.STRING_SCHEMA,message)
        blockingQueue.add(sourceRecord)
    }

    override def start(): Unit = {
        channel.basicConsume(settings.queue, true, this)
    }

    def getRecords(): java.util.List[SourceRecord] = {
        val recordsList = new util.LinkedList[SourceRecord]

        Option(blockingQueue.poll(settings.pollingTimeout,TimeUnit.MILLISECONDS)) match {
            case Some(x) =>
                recordsList.add(x)
                blockingQueue.drainTo(recordsList)
                recordsList
            case None =>
                //SourceTask.poll() requires null to be returned in case no data are present
                null
        }
    }
}

//object RabbitMQConsumer {
//    def apply(settings: RabbitMQClientSettings): RabbitMQConsumer = new RabbitMQConsumer(settings)
//}
