package com.datamountaineer.streamreactor.connect.rabbitmq.client

import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.rabbitmq.client._

abstract class RabbitMQClient(settings: RabbitMQSettings) {
    val connection: Connection = openNewConnection()
    val channels: List[Channel] = settings.kcql.map(e => connection.createChannel()).toList
    val sourcesToKcql = settings.kcql.map(e => e.getSource -> e).toMap
    val sourcesToChannels = sourcesToKcql.map(e => e._1 -> connection.createChannel())

    def start(): Unit

    def stop(): Unit = {
        channels.foreach(channel => channel.close())
        connection.close()
    }

    private def openNewConnection(): Connection = {
        val factory = new ConnectionFactory()
        factory.setHost(settings.hostname)
        factory.setPort(settings.port)
        factory.setUsername(settings.username)
        factory.setPassword(settings.password)

        factory.newConnection()
    }
}

