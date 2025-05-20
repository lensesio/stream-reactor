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
package io.lenses.streamreactor.connect.jms.source.converters

import io.lenses.streamreactor.connect.converters.source.Converter
import io.lenses.streamreactor.connect.jms.source.domain.JMSStructMessage
import org.apache.kafka.connect.source.SourceRecord

import jakarta.jms.Message

class CommonJMSMessageConverter(converter: Converter) extends JMSSourceMessageConverter {

  override def initialize(map: Map[String, String]): Unit =
    converter.initialize(map);

  override def convert(source: String, target: String, message: Message): SourceRecord =
    converter.convert(target, source, message.getJMSMessageID, JMSStructMessage.getPayload(message));
}
