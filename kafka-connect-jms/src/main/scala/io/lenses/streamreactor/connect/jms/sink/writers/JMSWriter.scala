/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.jms.sink.writers

import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.jms.JMSSessionProvider
import io.lenses.streamreactor.connect.jms.config.JMSSetting
import io.lenses.streamreactor.connect.jms.config.JMSSettings
import io.lenses.streamreactor.connect.jms.sink.converters.JMSHeadersConverterWrapper
import io.lenses.streamreactor.connect.jms.sink.converters.JMSSinkMessageConverter
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkRecord

import jakarta.jms._
import scala.annotation.nowarn
import scala.util.Failure
import scala.util.Success
import scala.util.Try

@nowarn("cat=deprecation")
case class JMSWriter(settings: JMSSettings)
    extends AutoCloseable
    with ConverterUtil
    with ErrorHandler
    with StrictLogging {

  val provider: JMSSessionProvider = JMSSessionProvider(settings, sink = true)
  provider.start()
  val producers: Map[String, MessageProducer] = provider.queueProducers ++ provider.topicProducers
  val converterMap: Map[String, JMSSinkMessageConverter] = settings.settings
    .map(s =>
      (
        s.source,
        JMSHeadersConverterWrapper(
          s.headers,
          s.converter.forSink.getOrElse(throw new ConfigException("No converter configured")),
        ),
      ),
    ).toMap
  val settingsMap: Map[String, JMSSetting] = settings.settings.map(s => (s.source, s)).toMap

  //initialize error tracker
  initialize(settings.retries, settings.errorPolicy)

  /**
    * Convert to a JMS record from a SinkRecord based
    * on the specified format in KCQL
    */
  def createJMSRecord(record: SinkRecord): (String, Message) = {
    val converter = converterMap(record.topic())
    converter.convert(record, provider.session, settingsMap(record.topic()))
  }

  /**
    * Write the records
    */
  def write(records: Seq[SinkRecord]): Option[Unit] = {
    //convert and send, commit the session if good
    val sent = Try {
      val messages = records.map(createJMSRecord)
      send(messages)
      provider.session.commit()
    }

    //rollback on failure
    sent match {
      case Failure(f) =>
        logger.error(s"Error processing messages, ${f.getMessage}")

        handleTry(Try(provider.session.rollback()))
        //handle error tracking for redelivery for Connect
        handleTry(sent)
      case Success(_) => None
    }
  }

  /**
    * Send the messages to the JMS destination
    */
  def send(messages: Seq[(String, Message)]): Unit =
    messages.foreach({ case (name, message) => producers(name).send(message) })

  override def close(): Unit = { val _ = provider.close() }
}
