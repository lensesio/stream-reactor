/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.connect.jms.sink.converters

import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.{TestBase, Using}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.collection.JavaConverters._

class ProtoDynamicConverterTest extends AnyWordSpec with Matchers with Using with TestBase with BeforeAndAfterAll {

  "ProtoDynamicConverter" should {
    "create a BytesMessage with sinkrecord payload" in {
      val converter = ProtoDynamicConverter()

      val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
      val queueName = UUID.randomUUID().toString
      val kcql = getKCQL(queueName, kafkaTopic1, "QUEUE")
      val props = getProps(kcql, JMS_URL)
      val config = JMSConfig(props.asJava)
      val settings = JMSSettings(config, true)
      val setting = settings.settings.head
      val schema = getSchema
      val struct = getStruct(schema)
      val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

      val convertedValue = converter.convert(record, setting)

      val stringValue = new String(convertedValue)

      Option(stringValue).isDefined shouldBe true
      stringValue shouldBe "int8: 12\n" +
        "int16: 12\n" +
        "int32: 12\n" +
        "int64: 12\n" +
        "float32: 12.2\n" +
        "float64: 12.2\n" +
        "boolean: true\n" +
        "string: \"foo\"\n" +
        "bytes: \"foo\"\n" +
        "array: \"a\"\n" +
        "array: \"b\"\n" +
        "array: \"c\"\n" +
        "map {\n  " +
        "key: \"field\"\n  " +
        "value: 1\n}\n" +
        "mapNonStringKeys {\n  key: 1\n  value: 1\n}\n"


    }
  }

}
