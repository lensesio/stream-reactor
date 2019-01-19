package com.datamountaineer.streamreactor.connect.rabbitmq.client

import java.io.IOException

import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.rabbitmq.client._
import com.typesafe.scalalogging.slf4j.StrictLogging

abstract class RabbitMQClient(settings: RabbitMQSettings) extends StrictLogging {
    val connection: Connection = openNewConnection()
    logger.info(s"Connected successfully to ${settings.host}:${settings.port}")
    val channels: List[Channel]

    def start(): Unit

    def stop(): Unit = {
        channels.foreach(channel => channel.close())
        connection.close()
    }

    private def openNewConnection(): Connection = {
        val factory = getConnectionFactory()
        factory.setHost(settings.host)
        factory.setPort(settings.port)
        factory.setUsername(settings.username)
        factory.setPassword(settings.password)

        factory.newConnection()
    }

    protected def getConnectionFactory(): ConnectionFactory = new ConnectionFactory()
}

