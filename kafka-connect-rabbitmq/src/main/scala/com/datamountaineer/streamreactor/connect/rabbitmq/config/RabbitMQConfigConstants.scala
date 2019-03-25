package com.datamountaineer.streamreactor.connect.rabbitmq.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._

object RabbitMQConfigConstants {
    val CONNECTOR_PREFIX = "connect.rabbitmq"

    //Default Configuration
    val HOST_CONFIG = s"${CONNECTOR_PREFIX}.${CONNECTION_HOST_SUFFIX}"
    val HOST_DOC = "Contains the RabbitMQ Server hostname or IP."
    val HOST_DISPLAY = "RabbitMQ connection endpoints"

    val KCQL_CONFIG = s"${CONNECTOR_PREFIX}.${KCQL_PROP_SUFFIX}"
    val KCQL_DOC = "Contains the Kafka Connect Query Language describing the sourced RabbitMQ source and the target Kafka topics"
    val KCQL_DISPLAY = "KCQL commands"

    //Optional Configuration
    val USER_CONFIG = s"${CONNECTOR_PREFIX}.${USERNAME_SUFFIX}"
    val USER_DOC = "Contains the RabbitMQ connection user name"
    val USER_DISPLAY = "Username"
    val USER_DEFAULT = "guest"

    val PASSWORD_CONFIG = s"${CONNECTOR_PREFIX}.${PASSWORD_SUFFIX}"
    val PASSWORD_DOC = "Contains the RabbitMQ connection password"
    val PASSWORD_DISPLAY = "Password"
    val PASSWORD_DEFAULT = "guest"

    val PORT_CONFIG = s"${CONNECTOR_PREFIX}.${CONNECTION_PORT_SUFFIX}"
    val PORT_DOC = "Contains the RabbitMQ Server port"
    val PORT_DISPLAY = "Port"
    val PORT_DEFAULT = 5672

    val VIRTUAL_HOST_CONFIG = s"${CONNECTOR_PREFIX}.virtual.host"
    val VIRTUAL_HOST_DOC = "Contains the endpoint of the broker"
    val VIRTUAL_HOST_DISPLAY = "Virtual Host"
    val VIRTUAL_HOST_DEFAULT = "/"

    val USE_TLS_CONFIG = s"${CONNECTOR_PREFIX}.use.tls"
    val USE_TLS_DOC = "Sets tls on/off. Default is on"
    val USE_TLS_DISPLAY = "Use TLS"
    val USE_TLS_DEFAULT = true

    val POLLING_TIMEOUT_CONFIG = s"${CONNECTOR_PREFIX}.polling.timeout"
    val POLLING_TIMEOUT_DOC = "Provides the timeout to poll incoming messages"
    val POLLING_TIMEOUT_DISPLAY = "Polling timeout"
    val POLLING_TIMEOUT_DEFAULT = 1000

    //Converters
    val DEFAULT_CONVERTER_CONFIG = s"${CONNECTOR_PREFIX}.source.default.converter"
    private[config] val DEFAULT_CONVERTER_DOC =
        """
          |Contains a canonical class name for the default converter of a raw JMS message bytes to a SourceRecord.
          |Overrides to the default can be done by using connect.jms.source.converters still.
          |i.e. com.datamountaineer.streamreactor.connect.source.converters.AvroConverter""".stripMargin
    private[config] val DEFAULT_CONVERTER_DISPLAY = "Default Converter class"
    private[config] val DEFAULT_CONVERTER_DEFAULT = ""

    val THROW_ON_CONVERT_ERRORS_CONFIG = s"${CONNECTOR_PREFIX}.converter.throw.on.error"
    private[config] val THROW_ON_CONVERT_ERRORS_DOC = "If set to false the conversion exception will be swallowed and everything carries on BUT the message is lost!!; true will throw the exception.Default is false."
    private[config] val THROW_ON_CONVERT_ERRORS_DISPLAY = "Throw error on conversion"
    private[config] val THROW_ON_CONVERT_ERRORS_DEFAULT = false

    val AVRO_CONVERTERS_SCHEMA_FILES_CONFIG = "connect.source.converter.avro.schemas"
    val AVRO_CONVERTERS_SCHEMA_FILES_DOC = "If the AvroConverter is used you need to provide an avro Schema to be able to read and translate the raw bytes to an avro record. The format is $QUEUE=$PATH_TO_AVRO_SCHEMA_FILE"
    val AVRO_CONVERTERS_SCHEMA_FILES_DISPLAY  = "Path to Avro Schema Files"
    val AVRO_CONVERTERS_SCHEMA_FILES_DEFAULT = ""

    object ConfigGroups {
        val CONNECTION = "Connection"
        val CONVERTERS = "Converters"
        val KCQL = "Kcql"
    }
}
