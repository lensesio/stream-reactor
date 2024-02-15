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

import io.lenses.streamreactor.connect.jms.config.JMSSetting
import org.apache.kafka.connect.sink.SinkRecord

import javax.jms.Message
import javax.jms.Session
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Try

class JMSHeadersConverterWrapper(headers: Map[String, String], delegate: JMSSinkMessageConverter)
    extends JMSSinkMessageConverter {

  override def convert(record: SinkRecord, session: Session, setting: JMSSetting): (String, Message) = {
    val response = delegate.convert(record, session, setting)
    val message  = response._2
    for (header <- record.headers().asScala) {
      message.setStringProperty(header.key(), Try(header.value().toString).toOption.orNull)
    }
    message.setStringProperty("JMSXGroupID", record.kafkaPartition().toString)
    for ((key, value) <- headers) {
      message.setStringProperty(key, value)
    }
    response
  }
}

object JMSHeadersConverterWrapper {
  def apply(config: Map[String, String], delegate: JMSSinkMessageConverter): JMSSinkMessageConverter =
    new JMSHeadersConverterWrapper(config, delegate)
}
