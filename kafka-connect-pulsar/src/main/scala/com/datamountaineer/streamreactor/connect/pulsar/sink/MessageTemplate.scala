package com.datamountaineer.streamreactor.connect.pulsar.sink

import org.apache.pulsar.client.api.{Producer, TypedMessageBuilder}

case class MessageTemplate (
  pulsarTopic: String,
  key: Option[String],
  value: Array[Byte],
  eventTime: Long,

                           ) {
  def toMessage(producer: Producer[Array[Byte]]): TypedMessageBuilder[Array[Byte]] = {
    val msg = producer.newMessage()
      .eventTime(eventTime)
      .value(value)

    key.map(e => msg.key(e))
    msg
  }
}
