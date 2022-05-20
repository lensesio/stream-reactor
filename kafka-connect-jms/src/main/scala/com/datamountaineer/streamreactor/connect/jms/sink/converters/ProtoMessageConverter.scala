package com.datamountaineer.streamreactor.connect.jms.sink.converters

import cats.implicits._
import com.datamountaineer.streamreactor.common.schemas.ConverterUtil
import com.datamountaineer.streamreactor.connect.jms.config.JMSSetting
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import javax.jms.BytesMessage
import javax.jms.JMSException
import javax.jms.Session
import scala.annotation.nowarn

@nowarn("cat=deprecation")
class ProtoMessageConverter extends JMSSinkMessageConverter with ConverterUtil with StrictLogging {

  private val dynamicConverter  = new ProtoDynamicConverter
  private val storedAsConverter = new ProtoStoredAsConverter

  override def initialize(map: Map[String, String]): Unit = {
    dynamicConverter.initialize(map)
    storedAsConverter.initialize(map)
  }

  override def convert(record: SinkRecord, session: Session, setting: JMSSetting): (String, BytesMessage) = {
    val protoConverter =
      if (setting.storageOptions.storedAs == null) dynamicConverter
      else storedAsConverter
    logger.debug(s"Proto converter loaded is: $protoConverter")
    (
      for {
        bytes   <- protoConverter.convert(record, setting)
        message <- writeMessage(session, setting.source, bytes)
      } yield message
    )
      .fold(
        ex => throw new ConnectException(ex.getMessage, ex),
        r => r,
      )
  }

  private def writeMessage(
    session: Session,
    source:  String,
    bytes:   Array[Byte],
  ): Either[JMSException, (String, BytesMessage)] =
    Either.catchOnly[JMSException] {
      val message = session.createBytesMessage
      message.writeBytes(bytes)
      source -> message
    }
}
