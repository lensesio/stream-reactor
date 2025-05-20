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
package io.lenses.streamreactor.connect.jms.sink.converters

import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.jms.config.JMSSetting
import org.apache.kafka.connect.sink.SinkRecord

import jakarta.jms.Message
import jakarta.jms.Session
import scala.annotation.nowarn

@nowarn("cat=deprecation")
class TextMessageConverter extends JMSSinkMessageConverter with ConverterUtil {

  override def convert(record: SinkRecord, session: Session, setting: JMSSetting): (String, Message) = {

    val value = record.value()
    val msg   = session.createTextMessage(value.asInstanceOf[String])

    (setting.source, msg)
  }
}
