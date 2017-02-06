/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.jms.sink.writer

import javax.jms.{Connection, Destination, MessageProducer, Session}
import javax.naming.InitialContext

import com.datamountaineer.streamreactor.connect.jms.sink.config.{JMSConfig, JMSSettings, TopicDestination}
import com.datamountaineer.streamreactor.connect.jms.sink.writer.converters.{JMSMessageConverter, JMSMessageConverterFn}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._
import scala.util.Try

case class JMSWriter(context: InitialContext,
                     connection: Connection,
                     routes: Seq[JMSConfig]) extends AutoCloseable with ConverterUtil with StrictLogging {
  val session: Session = connection.createSession(true, 0)

  val topicsMap: Map[String, (MessageProducer, JMSConfig)] = routes.map { route =>
    val destination = if (route.destinationType == TopicDestination) {
      session.createTopic(route.target)
    } else {
      session.createQueue(route.target)
    }
    route.source ->(session.createProducer(destination), route)
  }.toMap

  val converterMap: Map[String, JMSMessageConverter] = routes.map { route =>
    route.source->JMSMessageConverterFn(route.format)
  }.toMap

  def writeRecord(record: SinkRecord, cachedMappings: Map[String, Map[String, String]]): Map[String, Map[String, String]] = {
    topicsMap.get(record.topic()) match {
      case None =>
        //should never get here
        throw new IllegalArgumentException(s"${record.topic()} does not have a mapping in:{${topicsMap.keys.mkString(",")}}")
      case Some((producer, config)) =>
        val (msg, newCachedMappings) = if (config.includeAllFields) {
          if (config.fieldsAlias.isEmpty) {
            (converterMap(record.topic()).convert(record, session), cachedMappings)
          }
          else {
            val key = RecordKeyBuilderFn(record)
            cachedMappings.get(key) match {
              case None =>
                val map = record.valueSchema().fields().map(f => (f.name(), f.name())).toMap
                val cachedMap = config.fieldsAlias.foldLeft(map) { (m, e) => m + e }
                val newRecord = convert(record, cachedMap)
                (converterMap(record.topic()).convert(newRecord, session), cachedMappings + (key -> cachedMap))
              case Some(cachedMap) =>
                val newRecord = convert(record, cachedMap)
                (converterMap(record.topic()).convert(newRecord, session), cachedMappings)
            }
          }
        } else {
          val newRecord = convert(record, config.fieldsAlias)
          (converterMap(record.topic()).convert(newRecord, session), cachedMappings)
        }
        producer.send(msg)
        newCachedMappings
    }
  }

  def write(records: Seq[SinkRecord]): Unit = {
    try {
      val (count, _) = records.foldLeft((0, Map.empty[String, Map[String, String]])) { case ((total, map), record) =>
        //(total + 1, writeRecord(record, map))
        (total + 1, writeRecord(record, map))
      }
      logger.debug(s"Writing ${count + 1} records to JMS...")
      session.commit()
    }
    catch {
      case _: Throwable =>
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

    JMSWriter(context, connection, settings.routes)
  }
}

object RecordKeyBuilderFn {
  def apply(record: SinkRecord): String = {
    s"${record.topic()}.${record.valueSchema().name()}.${record.valueSchema().version()}"
  }
}
