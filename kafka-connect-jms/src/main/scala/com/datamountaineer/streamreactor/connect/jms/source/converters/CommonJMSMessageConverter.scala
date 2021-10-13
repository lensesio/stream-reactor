package com.datamountaineer.streamreactor.connect.jms.source.converters

import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.jms.source.domain.JMSStructMessage
import org.apache.kafka.connect.source.SourceRecord

import javax.jms.Message


class CommonJMSMessageConverter(converter: Converter) extends JMSMessageConverter {


  override def initialize(map: Map[String, String]): Unit = {
    converter.initialize(map);
  }

  override def convert(source: String, target: String, message: Message): SourceRecord = {
    converter.convert(target, source, message.getJMSMessageID, JMSStructMessage.getPayload(message));
  }
}

