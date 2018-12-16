package com.datamountaineer.streamreactor.connect.rabbitmq.client

import java.util
import java.util.Collections
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.rabbitmq.client._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord

class RabbitMQConsumer(settings: RabbitMQSettings) extends RabbitMQClient(settings) with StrictLogging {
    private val blockingQueue = new LinkedBlockingQueue[SourceRecord]()
    logger.info(s"Connecting consumer to ${settings.hostname}:${settings.port}")
//    val queues = kcql.map(e => e.getSource)

    override def start(): Unit = {
        //Does this override the previous queue settings if it already exists?
//        queues.foreach(q => channel.queueDeclare(q,false,false,false,null))
        sourcesToChannels.foreach(e => {
            val queue = e._1
            val channel = e._2
            sourcesToKcql.get(queue) match {
                case Some(x) => {
                    channel.queueDeclare(queue,false,false,false,null)
                    channel.basicConsume(queue,settings.AUTO_ACK_MESSAGES, new QueueConsumer(channel,queue,x))
                }
                case None => logger.error(s"Cannot get kcql configuration for queue $queue")
            }
        });
    }

    def getRecords(): Option[java.util.List[SourceRecord]] = {
        Option(blockingQueue.poll(settings.pollingTimeout,TimeUnit.MILLISECONDS)) match {
            case Some(x) =>
                val recordsList = new util.LinkedList[SourceRecord]
                recordsList.add(x)
                blockingQueue.drainTo(recordsList)
                Option(recordsList)
            case None =>
                None
        }
    }

    class QueueConsumer(channel: Channel,queue: String,kcql: Kcql) extends Consumer {
        val converter = settings.sourcesToConvertersMap.getOrElse(queue, throw new RuntimeException(s"Queue $queue is missing the converter instance."))

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
            val kafkaTopic = kcql.getTarget
            val messageId = Option(properties.getMessageId) match {
                case Some(e) => properties.getMessageId
                case None => {
                    import java.util.UUID.randomUUID
                    randomUUID().toString
                }
            }

            val sourceRecord = converter.convert(kafkaTopic,queue,messageId, body)
//            var message = new String(body, "UTF-8")
//            val sourceRecord = new SourceRecord(Collections.singletonMap("channel",queue),Collections.singletonMap("offset",1),kafkaTopic,
//                null,null,null,Schema.BYTES_SCHEMA,body)
            blockingQueue.add(sourceRecord)
            //Acknowledge that the message arrived
            channel.basicAck(envelope.getDeliveryTag,false)
        }
    }
}

object RabbitMQConsumer {
    def apply(settings: RabbitMQSettings): RabbitMQConsumer = new RabbitMQConsumer(settings)
}
