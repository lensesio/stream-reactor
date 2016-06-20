package com.datamountaineer.streamreactor.connect.jms.sink.writer.converters

import javax.jms.{Message, Session}

import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._

class JsonMessageConverter extends JMSMessageConverter with ConverterUtil {
  jsonConverter.configure(Map.empty[String, String], false)

  override def convert(sinkRecord: SinkRecord, session: Session): Message = {
    val json = this.convertValueToJson(sinkRecord)
    session.createTextMessage(json.toString)
  }
}
