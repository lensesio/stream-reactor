package com.datamountaineer.streamreactor.connect.jms.sink.writer.converters

import javax.jms.{Message, Session}

import org.apache.kafka.connect.sink.SinkRecord


trait JMSMessageConverter {
  def convert(record:SinkRecord, session:Session):Message
}
