package com.datamountaineer.streamreactor.connect.jms.source.converters

import com.datamountaineer.streamreactor.connect.jms.source.domain.JMSStructMessage
import org.apache.kafka.connect.source.SourceRecord

import javax.jms.Message


class JMSStructMessageConverter extends JMSMessageConverter {

  def convert(source: String, target: String, message: Message): SourceRecord = {
    JMSStructMessage.getStruct(target, message)
  }
}

