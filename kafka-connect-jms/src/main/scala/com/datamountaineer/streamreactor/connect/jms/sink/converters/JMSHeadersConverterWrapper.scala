package com.datamountaineer.streamreactor.connect.jms.sink.converters
import com.datamountaineer.streamreactor.connect.jms.config.JMSSetting
import javax.jms.{Message, Session}
import org.apache.kafka.connect.sink.SinkRecord


class JMSHeadersConverterWrapper(headers: Map[String, String], delegate: JMSMessageConverter) extends JMSMessageConverter {


  override def convert(record: SinkRecord, session: Session, setting: JMSSetting): (String, Message) = {
    val response = delegate.convert(record, session, setting)
    val message = response._2
    for((key, value) <- headers) {
      message.setObjectProperty(key, value)
    }
    response
  }
}


object JMSHeadersConverterWrapper {
  def apply(config: Map[String, String], delegate: JMSMessageConverter): JMSMessageConverter =
    new JMSHeadersConverterWrapper(config, delegate)
}
