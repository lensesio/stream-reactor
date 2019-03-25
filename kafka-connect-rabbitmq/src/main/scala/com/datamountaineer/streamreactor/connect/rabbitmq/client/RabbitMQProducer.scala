package com.datamountaineer.streamreactor.connect.rabbitmq.client

import java.io.IOException

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.{FieldConverter, Transform}
import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.rabbitmq.client.Channel
import io.confluent.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._

class RabbitMQProducer(settings: RabbitMQSettings) extends RabbitMQClient(settings) {
    private val kafkaTopicsToKcql = settings.kcql.map(e => e.getSource -> e).toMap
    private val exchangesToChannels = settings.kcql.map(e => e.getTarget -> connection.createChannel()).toMap
    private val exchangesToKcql = settings.kcql.map(e => e.getTarget -> e).toMap
    override val channels = exchangesToChannels.map(e => e._2).toList

    override def start(): Unit = {
        exchangesToChannels.foreach(e => {
            val exchange = e._1
            val channel = e._2
            exchangesToKcql.get(exchange) match {
                case Some(kcql) => {
                    try {
                        val checkExchangeChannel = connection.createChannel()
                        checkExchangeChannel.exchangeDeclarePassive(exchange)
                        logger.info(s"Exchange $exchange already exists. Keeping the existing exchange settings")
                        checkExchangeChannel.close()
                    } catch {
                        case e: IOException => {
                            Option(kcql.getWithType) match {
                                case Some(exchangeType) => {
                                    if (settings.EXCHANGE.TYPES.contains(exchangeType))  {
                                        channel.exchangeDeclare(exchange, exchangeType, settings.EXCHANGE.DURABLE, settings.EXCHANGE.AUTO_DELETE, settings.EXCHANGE.INTERNAL, null)
                                        logger.info(s"Exchange $exchange not found. Creating a new one with default settings")
                                    } else {
                                        logger.error(s"Exchange $exchange cannot be created. Unsupported WITHTYPE provided. Supported types are {fanout,direct,topic}")
                                    }

                                }
                                case None => logger.error(s"Exchange $exchange cannot be created. Please provide the exchange type in the WITHTYPE attribute.")
                            }
                        }
                    }
                }
                case None => logger.error(s"Cannot get kcql configuration for exchange $exchange")
            }
        })
    }

    def write(records: List[SinkRecord]): Unit = {
        val recordGroups = records.groupBy(record =>
            kafkaTopicsToKcql.getOrElse(record.topic(), logger.error(s"Kafka topic ${record.topic()} does not have a " +
                s"corresponding KCQL")))

        recordGroups.map({
            case (kcql: Kcql, records) => {
                val currentChannel = exchangesToChannels.getOrElse(kcql.getTarget, throw new ConfigException("No channel found"))
                sendMessages(records,currentChannel,kcql)
            }
        })
    }

    private def sendMessages(records: List[SinkRecord],channel:Channel,kcql: Kcql) {
        val exchange = kcql.getTarget
        val routingKey = Option(kcql.getTags) match {
            case Some(tags) => tags.get(0).getKey
            case None => ""
        }

        records.foreach(record => {
            val message = Transform(
                kcql.getFields.map(FieldConverter.apply),
                kcql.getIgnoredFields.map(FieldConverter.apply),
                record.valueSchema(),
                record.value(),
                kcql.hasRetainStructure())

            channel.basicPublish(exchange, routingKey, null, message.getBytes("UTF-8"))
        })
    }
}

object RabbitMQProducer {
    def apply(settings: RabbitMQSettings) = new RabbitMQProducer(settings)
}
