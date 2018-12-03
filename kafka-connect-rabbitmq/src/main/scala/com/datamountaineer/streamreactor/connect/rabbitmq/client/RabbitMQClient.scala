package com.datamountaineer.streamreactor.connect.rabbitmq.client

import com.datamountaineer.streamreactor.connect.rabbitmq.config.RabbitMQSettings
import com.rabbitmq.client._

abstract class RabbitMQClient(settings: RabbitMQSettings) {
    val connection: Connection = openNewConnection()
    val channel: Channel = createChannel()

    def start(): Unit

    def stop(): Unit = {
        channel.close()
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

    private def createChannel(): Channel = {
        val channel = connection.createChannel()
        channel.queueDeclare("queue01",false,false,false,null)

        channel
    }
}

