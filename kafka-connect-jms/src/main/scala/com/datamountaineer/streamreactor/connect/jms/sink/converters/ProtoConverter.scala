package com.datamountaineer.streamreactor.connect.jms.sink.converters

import com.datamountaineer.streamreactor.connect.jms.config.JMSSetting
import org.apache.kafka.connect.sink.SinkRecord

import java.io.IOException
import java.util

trait ProtoConverter {
  def initialize(map: util.Map[String, String]): Unit = {}

  @throws[IOException]
  def convert(record: SinkRecord, setting: JMSSetting): Array[Byte]
}
