package com.datamountaineer.streamreactor.connect.jms.sink.writer.converters

import com.datamountaineer.streamreactor.connect.jms.sink.config.MessageType

object JMSMessageConverterFn {
  def apply(messageType: MessageType): JMSMessageConverter = {
    messageType match {
      case MessageType.AVRO => new AvroMessageConverter
      case MessageType.JSON => new JsonMessageConverter
      case MessageType.OBJECT => new ObjectMessageConverter
      case MessageType.MAP => new MapMessageConverter
    }
  }
}
