package com.datamountaineer.streamreactor.connect.jms.sink.writer

import javax.jms.{Connection, Destination}
import javax.naming.InitialContext

import com.datamountaineer.streamreactor.connect.jms.sink.config.{JMSConfig, JMSSettings, TopicDestination}
import com.datamountaineer.streamreactor.connect.jms.sink.writer.converters.{JMSMessageConverter, JMSMessageConverterFn}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try

case class JMSWriter(context: InitialContext,
                     connection: Connection,
                     converter: JMSMessageConverter,
                     routes: Seq[JMSConfig]) extends AutoCloseable with ConverterUtil with StrictLogging {
  val session = connection.createSession(true, 0)

  val topicsMap = routes.map { route =>
    val destination = if (route.destinationType == TopicDestination) {
      session.createTopic(route.target)
    } else {
      session.createQueue(route.target)
    }
    route.source ->(session.createProducer(destination), route.fieldsAlias)
  }.toMap


  def writeRecord(record: SinkRecord): Unit = {
    topicsMap.get(record.topic()) match {
      case None =>
        //should never get here
        throw new IllegalArgumentException(s"${record.topic()} does not have a mapping in:{${topicsMap.keys.mkString(",")}}")
      case Some((producer, mappings)) =>
        val newRecord = extractSinkFields(record, mappings)
        val msg = converter.convert(newRecord, session)
        producer.send(msg)
    }
  }

  def write(records: Seq[SinkRecord]): Unit = {
    try {
      val count = records.foldLeft(0) { (total, record) =>
        writeRecord(record)
        total + 1
      }
      logger.info(s"Writing ${count + 1} records to JMS...")
      session.commit()
    }
    catch {
      case t: Throwable =>
        val first = records.head
        logger.error(s"There was an error writing records from topic:${first.topic()}  partition:${first.kafkaPartition()} offset:${first.kafkaOffset()}")
        session.rollback()
    }
  }

  override def close(): Unit = {
    Try(session.close())
    Try(connection.close())
    Try(context.close())
  }
}

case class DestinationAndFieldAlias(destination: Destination, fieldAlias: Map[String, String])

object JMSWriter {
  def apply(settings: JMSSettings): JMSWriter = {
    val context = new InitialContext()
    val connectionFactory = settings.connectionFactoryClass.getConstructor(classOf[String]).newInstance(settings.connectionURL)

    val connection = settings.user match {
      case None => connectionFactory.createConnection()
      case Some(user) => connectionFactory.createConnection(user, settings.password.get)
    }

    JMSWriter(context, connection, JMSMessageConverterFn(settings.messageType), settings.routes)
  }
}