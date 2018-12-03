package com.datamountaineer.streamreactor.connect.rabbitmq.config

case class RabbitMQSettings(hostname: String,
                            port: Int,
                            username: String,
                            password: String,
                            queue: String,
                            exchange: String,
                            kafkaTopic: String,
                            virtalHost: String,
                            pollingTimeout: Long) {
}

object RabbitMQSettings {
    def apply(config: RabbitMQConfig): RabbitMQSettings = {
        val hostname = config.getHost
        val port = config.getPort
        val username = config.getUsername
        val password = config.getSecret.value()
        val queue = "queue01"
        val exchange = ""
        val kafkaTopic = config.getString(RabbitMQConfigConstants.TOPIC_CONFIG)
        val virtualHost = config.getString(RabbitMQConfigConstants.VIRTUAL_HOST_CONFIG)
        val pollingTimeout = config.getLong(RabbitMQConfigConstants.POLLING_TIMEOUT_CONFIG)

        RabbitMQSettings(hostname,port,username,password,queue,exchange,kafkaTopic,virtualHost,pollingTimeout)
    }
}
