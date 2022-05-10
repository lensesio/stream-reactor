package com.datamountaineer.streamreactor.connect.jms.sink.converters

import com.datamountaineer.streamreactor.connect.jms.config.JMSSetting
import org.apache.kafka.connect.sink.SinkRecord

import java.io.IOException

trait ProtoConverter {
  def initialize(map: Map[String, String]): Unit = {}

  def convert(record: SinkRecord, setting: JMSSetting): Either[IOException, Array[Byte]]
}
