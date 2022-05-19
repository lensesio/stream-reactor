package com.datamountaineer.streamreactor.connect.jms.source.converters

import com.datamountaineer.streamreactor.connect.jms.converters.JMSMessageConverter
import org.apache.kafka.connect.source.SourceRecord

import javax.jms.Message

trait JMSSourceMessageConverter extends JMSMessageConverter {

  def convert(source: String, target: String, message: Message): SourceRecord
}
