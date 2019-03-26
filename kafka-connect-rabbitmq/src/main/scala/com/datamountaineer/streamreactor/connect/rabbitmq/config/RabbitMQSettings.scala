package com.datamountaineer.streamreactor.connect.rabbitmq.config

import java.util

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.source.{BytesConverter, Converter}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException

import scala.util.{Failure, Success, Try}

case class RabbitMQSettings(host: String,
                            port: Int,
                            username: String,
                            password: String,
                            virtualHost: String,
                            kcql: Set[Kcql],
                            sourcesToConvertersMap: Map[String,Converter],
                            useTls: Boolean,
                            pollingTimeout: Long) {
    val AUTO_ACK_MESSAGES = false
    object QUEUE {
        val DURABLE = true
        val EXCLUSIVE = false
        val AUTO_DELETE = false
    }
    object EXCHANGE {
        val DURABLE = true
        val AUTO_DELETE = false
        val INTERNAL = false
        val TYPES = List("fanout","direct","topic")
    }
}

object RabbitMQSettings extends StrictLogging {
    def apply(props: util.Map[String, String]): RabbitMQSettings = {
        val config = RabbitMQConfig(props)
        val host = config.getHost
        val port = config.getPort match {
            case x if 1 to 65535 contains x => x
            case x => throw new ConfigException(s"${RabbitMQConfigConstants.PORT_CONFIG} should be between [1,65535]. Current value is $x")
        }
        val username = config.getUsername
        val password = config.getSecret.value()
        val virtualHost = config.getString(RabbitMQConfigConstants.VIRTUAL_HOST_CONFIG)
        val pollingTimeout = config.getLong(RabbitMQConfigConstants.POLLING_TIMEOUT_CONFIG) match {
            case x if x <= 0 => throw new ConfigException(s"${RabbitMQConfigConstants.POLLING_TIMEOUT_CONFIG} should be positive. Current value is $x")
            case x => x
        }
        val kcql = config.getKCQL

        val useTls = config.getBoolean(RabbitMQConfigConstants.USE_TLS_CONFIG)

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

        RabbitMQSettings(host,port,username,password,virtualHost,kcql,sourcesToConvertersMap,useTls,pollingTimeout)
    }
}
