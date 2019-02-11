package com.datamountaineer.streamreactor.connect.rabbitmq.client

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
    override val channels = exchangesToChannels.map(e => e._2).toList
    //    private val exchangesToKcql = settings.kcql.map(e => e.getTarget -> e).toMap
    //    private val kafkaTopicsToExchanges = exchangesToKcql.map(e => e._2.getSource -> e._1)

    override def start(): Unit = {

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
        val routingKey = Option(kcql.getWithKeys.get(0)) match {
            case Some(x) => x
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

//                exchangesToChannels.get(kcql.getTarget) match {
//                    case Some(channel) => {
//                        records.foreach(record => {
//                            val message = Transform(
//                                kcql.getFields.map(FieldConverter.apply),
//                                kcql.getIgnoredFields.map(FieldConverter.apply),
//                                record.valueSchema(),
//                                record.value(),
//                                kcql.hasRetainStructure())
//
//                            channel.basicPublish(kcql.getTarget, "", null, message.getBytes("UTF-8"))
//                        })
//                    }
//                    case None => logger.error("No channel found")
//                }
//            }
//        val json = Transform(
//            kcql.getFields.map(FieldConverter.apply),
//            kcql.getIgnoredFields.map(FieldConverter.apply),
//            records.valueSchema(),
//            records.value(),
//            kcql.hasRetainStructure())
//        recordGroups.foreach(group => )
//        records.foreach(record => {
//            kafkaTopicsToExchanges.get(record.topic()) match {
//                case Some(exchange) => {
//
//                }
//                case None => logger.error(s"Kafka topic ${record.topic()} does not have a corresponding exchange")
//            }
//            val channel = exchangesToChannels.get(exchange)
//            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"))
//        })
}

object RabbitMQProducer {
    def apply(settings: RabbitMQSettings) = new RabbitMQProducer(settings)
}
