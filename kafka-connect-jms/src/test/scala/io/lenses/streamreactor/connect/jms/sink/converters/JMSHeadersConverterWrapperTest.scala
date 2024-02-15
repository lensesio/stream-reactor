/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.jms.sink.converters

import io.lenses.kcql.FormatType
import io.lenses.streamreactor.connect.jms.config.JMSSetting
import io.lenses.streamreactor.connect.jms.config.SinkConverterConfigWrapper
import io.lenses.streamreactor.connect.jms.config.StorageOptions
import io.lenses.streamreactor.connect.jms.config.TopicDestination
import org.apache.activemq.command.ActiveMQObjectMessage
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.when
import org.scalatest.funsuite.AnyFunSuiteLike
import java.lang.{ Iterable => JavaIterable }
import javax.jms.Session
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.IteratorHasAsScala

class JMSHeadersConverterWrapperTest extends AnyFunSuiteLike {

  test("should handle missing values in headers") {

    val jmsSession: Session              = mockSession
    val setting:    JMSSetting           = createSettings
    val headers:    JavaIterable[Header] = createHeaders
    val sinkRecord = new SinkRecord(
      "myTopic",
      1,
      null,
      null,
      Schema.STRING_SCHEMA,
      "myValue",
      1,
      null,
      null,
      headers,
    )

    JMSHeadersConverterWrapper(Map.empty, new ObjectMessageConverter())
      .convert(sinkRecord, jmsSession, setting)

  }

  private def createSettings = {
    val setting: JMSSetting = JMSSetting(
      source           = "mySource",
      target           = "myTarget",
      fields           = Map(),
      ignoreField      = Set(),
      destinationType  = TopicDestination,
      format           = FormatType.JSON,
      storageOptions   = StorageOptions("JSON", Map.empty),
      converter        = SinkConverterConfigWrapper(new TextMessageConverter {}),
      messageSelector  = Option.empty,
      subscriptionName = Option.empty,
      headers          = Map.empty,
    )
    setting
  }

  private def createHeaders = {
    val headers = new ConnectHeaders()
    headers.add("JMSReplyTo", null)
    headers.add("Destination", new SchemaAndValue(Schema.STRING_SCHEMA, "topic://myFunTopicName"))

    headers.iterator().asScala.iterator.to(Iterable).asJava
  }

  private def mockSession = {
    val objectMessage = new ActiveMQObjectMessage

    val jmsSession = mock[Session]
    when(jmsSession.createObjectMessage()).thenReturn(objectMessage)
    when(jmsSession.createObjectMessage(any[Serializable])).thenReturn(objectMessage)
    jmsSession
  }
}
