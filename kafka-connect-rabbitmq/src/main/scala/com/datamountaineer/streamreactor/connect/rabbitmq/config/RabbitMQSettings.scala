package com.datamountaineer.streamreactor.connect.rabbitmq.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.source.{BytesConverter, Converter}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._

case class RabbitMQSettings(hostname: String,
                            port: Int,
                            username: String,
                            password: String,
                            virtualHost: String,
                            kcql: Set[Kcql],
                            sourcesToConvertersMap: Map[String,Converter],
                            pollingTimeout: Long) {
    val AUTO_ACK_MESSAGES = false
    val DURABLE_QUEUE = true
}

object RabbitMQSettings extends StrictLogging {
    def apply(config: RabbitMQConfig): RabbitMQSettings = {
        val hostname = config.getHost
        val port = config.getPort
        val username = config.getUsername
        val password = config.getSecret.value()
        val virtualHost = config.getString(RabbitMQConfigConstants.VIRTUAL_HOST_CONFIG)
        val pollingTimeout = config.getLong(RabbitMQConfigConstants.POLLING_TIMEOUT_CONFIG)

        val kcql = config.getKCQL

        val converters = kcql.map(k => {
            (k.getSource, if (k.getWithConverter == null) classOf[BytesConverter].getCanonicalName else k.getWithConverter)
        }).toMap

        converters.foreach( {
            case (rabbitMQSource, clazz) => {
                Try(Class.forName(clazz)) match {
                    case Failure(_) => throw new ConfigException(s"Invalid ${RabbitMQConfigConstants.KCQL_CONFIG}. $clazz can't be found for $rabbitMQSource")
                    case Success(clz) =>
                        if (!classOf[Converter].isAssignableFrom(clz)) {
                            throw new ConfigException(s"Invalid ${RabbitMQConfigConstants.KCQL_CONFIG}. $clazz is not inheriting Converter for $rabbitMQSource")
                        }
                }
            }
        })

        val sourcesToConvertersMap = converters.map { case (topic, clazz) =>
            logger.info(s"Creating converter instance for $clazz")
            val converter = Try(Class.forName(clazz).newInstance()) match {
                case Success(value) => value.asInstanceOf[Converter]
                case Failure(_) => throw new ConfigException(s"Invalid ${RabbitMQConfigConstants.KCQL_CONFIG} is invalid. $clazz should have an empty ctor!")
            }
            import scala.collection.JavaConverters._
            converter.initialize(config.props.asScala.toMap)
            topic -> converter
        }

        RabbitMQSettings(hostname,port,username,password,virtualHost,kcql,sourcesToConvertersMap,pollingTimeout)
    }
}
