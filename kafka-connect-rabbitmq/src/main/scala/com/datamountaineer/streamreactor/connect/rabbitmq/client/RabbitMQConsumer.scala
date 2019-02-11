package com.datamountaineer.streamreactor.connect.rabbitmq.client

import java.io.IOException
import java.util
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.rabbitmq.client._
import org.apache.kafka.connect.source.SourceRecord

class RabbitMQConsumer(settings: RabbitMQSettings) extends RabbitMQClient(settings) {
    private val queuesToKcql = settings.kcql.map(e => e.getSource -> e).toMap
    private val queuesToChannels = queuesToKcql.map(e => e._1 -> connection.createChannel())
    override val channels = queuesToChannels.map(e => e._2).toList
    private val blockingQueue = new LinkedBlockingQueue[SourceRecord]()

    override def start(): Unit = {
        queuesToChannels.foreach(e => {
            val queue = e._1
            val channel = e._2
            queuesToKcql.get(queue) match {
                case Some(x) => {
                    try {
                        val checkQueueChannel = connection.createChannel()
                        checkQueueChannel.queueDeclarePassive(queue)
                        logger.info(s"Queue $queue already created. Keeping the existing queue settings")
                        checkQueueChannel.close()
                    } catch {
                        case e: IOException => {
                            channel.queueDeclare(queue, settings.QUEUE.DURABLE, settings.QUEUE.EXCLUSIVE, settings.QUEUE.AUTO_DELETE, null)
                            logger.info(s"Queue $queue not found. Creating a new one with default settings")
                        }
                    }
                    channel.basicConsume(queue,settings.AUTO_ACK_MESSAGES, new QueueConsumer(channel,queue,x))
                }
                case None => logger.error(s"Cannot get kcql configuration for queue $queue")
            }
        })
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

    private class QueueConsumer(channel: Channel,queue: String,kcql: Kcql) extends Consumer {
        val converter = settings.sourcesToConvertersMap.getOrElse(queue, throw new RuntimeException(s"Queue $queue is missing the converter instance."))

        override def handleConsumeOk(consumerTag: String): Unit = {
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

            val sourceRecord = converter.convert(kafkaTopic,queue,messageId,body)
            blockingQueue.add(sourceRecord)
            //Acknowledge that the message arrived
            channel.basicAck(envelope.getDeliveryTag,false)
        }
    }
}

object RabbitMQConsumer {
    def apply(settings: RabbitMQSettings) = new RabbitMQConsumer(settings)
}
