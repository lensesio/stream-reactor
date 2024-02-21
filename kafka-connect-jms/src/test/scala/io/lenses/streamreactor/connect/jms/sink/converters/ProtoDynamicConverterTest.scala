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

import io.lenses.streamreactor.connect.jms.config.JMSConfig
import io.lenses.streamreactor.connect.jms.config.JMSSettings
import io.lenses.streamreactor.connect.jms.TestBase
import io.lenses.streamreactor.example.AddressedPerson
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

class ProtoDynamicConverterTest
    extends AnyWordSpec
    with Matchers
    with TestBase
    with BeforeAndAfterAll
    with EitherValues {

  "ProtoDynamicConverter" should {
    "create a BytesMessage with sinkrecord payload" in {
      val converter = ProtoDynamicConverter()

      val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
      val queueName   = UUID.randomUUID().toString
      val kcql        = getKCQL(queueName, kafkaTopic1, "QUEUE")
      val props       = getProps(kcql, JMS_URL)
      val config      = JMSConfig(props)
      val settings    = JMSSettings(config, true)
      val setting     = settings.settings.head
      val schema      = getProtobufSchema
      val struct      = getProtobufStruct(schema, "lenses", 101, "lenses@lenses.com")
      val record      = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

      val convertedValue = converter.convert(record, setting)

      val person = AddressedPerson.parser().parseFrom(convertedValue.value)

      person.getName shouldBe "lenses"
      person.getId shouldBe 101
      person.getEmail shouldBe "lenses@lenses.com"

    }
  }

}
