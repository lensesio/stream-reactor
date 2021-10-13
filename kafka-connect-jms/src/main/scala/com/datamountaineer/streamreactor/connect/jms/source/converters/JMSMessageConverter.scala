package com.datamountaineer.streamreactor.connect.jms.source.converters

import org.apache.kafka.connect.source.SourceRecord

import javax.jms.Message


trait JMSMessageConverter {
  def initialize(config: Map[String, String]): Unit = {}

  def convert(source: String, target: String, message: Message): SourceRecord
}

