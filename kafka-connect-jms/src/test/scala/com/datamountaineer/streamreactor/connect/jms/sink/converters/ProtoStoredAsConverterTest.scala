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
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.collection.JavaConverters._

class ProtoStoredAsConverterTest extends AnyWordSpec with Matchers with Using with TestBase with BeforeAndAfterAll {

  "create a BytesMessage with sinkrecord payload with storedAs properties" in {
    val converter = ProtoStoredAsConverter()

    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val path = getClass.getClassLoader.getResource("proto/NonAddressedPerson.proto").getPath
      .replace("/NonAddressedPerson.proto", "")

    val kcql = getKCQLStoreAsTimedPerson(queueName, kafkaTopic1, "QUEUE", path)
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchemaTimestamp
    val struct = getProtobufStructTimestamp(schema, "non-addressed-person", 101, "1970-01-01T00:00:00Z")
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, true)
    val setting = settings.settings.head

    converter.initialize(props.asJava)
    val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

    val convertedValue: Array[Byte] = converter.convert(record, setting)

    val stringValue = convertedValue.map(_.toChar).mkString

    Option(stringValue).isDefined shouldBe true
    stringValue.contains("name") shouldBe true
    stringValue.contains("non-addressed-person") shouldBe true
    stringValue.contains("id") shouldBe true
    stringValue.contains("101") shouldBe true
    stringValue.contains("timestamp") shouldBe true
    stringValue.contains("1970-01-01T00:00:00Z") shouldBe true

  }

  "create a BytesMessage with sinkrecord payload with connector config proto path properties" in {
    val converter = ProtoStoredAsConverter()

    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val path = getClass.getClassLoader
      .getResource("proto/AddressedPerson.proto")
      .getPath
      .replace("AddressedPerson.proto", "")
    val kcql = getKCQLEmptyStoredAsNonAddressedPerson(queueName, kafkaTopic1, "QUEUE")
    val props = getProps(kcql, JMS_URL) ++
      Map("connect.sink.converter.proto_path" -> path)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "non-addrressed-person", 102, "non-addressed-person@gmail.com")
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, sink = true)
    val setting = settings.settings.head

    converter.initialize(props.asJava)
    val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

    val convertedValue: Array[Byte] = converter.convert(record, setting)

    val stringValue = convertedValue.map(_.toChar).mkString
    Option(stringValue).isDefined shouldBe true
    stringValue.contains("name") shouldBe true
    stringValue.contains("non-addressed-person@gmail.com") shouldBe true
    stringValue.contains("id") shouldBe true
    stringValue.contains("102") shouldBe true
    stringValue.contains("email") shouldBe true
    stringValue.contains("non-addressed-person@gmail.com") shouldBe true
  }

  "create a BytesMessage with sinkrecord payload with only storedAs Name" in {
    val converter = ProtoStoredAsConverter()

    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQLStoredAsWithNameOnly(queueName, kafkaTopic1, "QUEUE")
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, sink = true)
    val setting = settings.settings.head

    converter.initialize(props.asJava)
    val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

    val convertedValue: Array[Byte] = converter.convert(record, setting)

    val stringValue = convertedValue.map(_.toChar).mkString

    Option(stringValue).isDefined shouldBe true
    stringValue.contains("name") shouldBe true
    stringValue.contains("addressed-person@gmail.com") shouldBe true
    stringValue.contains("id") shouldBe true
    stringValue.contains("103") shouldBe true
    stringValue.contains("email") shouldBe true
    stringValue.contains("addressed-person@gmail.com") shouldBe true

  }

  "should throw exception for invalid value for storedAs" in {
    val converter = ProtoStoredAsConverter()

    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQLStoredAsWithInvalidData(queueName, kafkaTopic1, "QUEUE")
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, sink = true)
    val setting = settings.settings.head

    converter.initialize(props.asJava)
    val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

    try {
      converter.convert(record, setting)
    } catch {
      case x: DataException =>
        assert(x.getMessage == "Invalid storedAs settings: NonAddressedPersonOuterClass")
    }

  }

  "should throw exception for invalid package name for storedAs when protopath is present" in {
    val converter = ProtoStoredAsConverter()

    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val path = getClass.getClassLoader.getResource("proto/NonAddressedPerson.proto").getPath
      .replace("/NonAddressedPerson.proto", "")
    val kcql = getKCQLStoredAsWithInvalidPackageNameWithProtopath(queueName, kafkaTopic1, "QUEUE", path)
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, sink = true)
    val setting = settings.settings.head

    converter.initialize(props.asJava)
    val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

    try {
      converter.convert(record, setting)
    } catch {
      case x: DataException =>
        assert(x.getMessage == "Invalid storedAs settings: Proto file package name doesn't match with storedAs package name")
    }

  }

  "should throw exception for valid package name for storedAs but invalid protopath which has no files in it" in {
    val converter = ProtoStoredAsConverter()

    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString

    val kcql = getKCQLStoredAsWithProtopath(queueName, kafkaTopic1, "QUEUE", "/resources/path")
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, true)
    val setting = settings.settings.head

    converter.initialize(props.asJava)
    val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

    try {
      converter.convert(record, setting)
    } catch {
      case x: DataException =>
        println(x.getMessage)
        assert(x.getMessage == "Invalid storedAs settings: /resources/path")
    }

  }

  "should throw exception for for incorrect proto file name" in {
    val converter = ProtoStoredAsConverter()

    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val path = getClass.getClassLoader.getResource("proto/NonAddressedPerson.proto").getPath
      .replace("/NonAddressedPerson.proto", "")
    val kcql = getKCQLStoreAsWithFileAndPath(queueName, kafkaTopic1, "QUEUE", "`NonExisting.proto`",path)
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, sink = true)
    val setting = settings.settings.head

    converter.initialize(props.asJava)
    val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

    try {
      converter.convert(record, setting)
    } catch {
      case x: DataException =>
        println(x.getMessage)
        assert(x.getMessage == "Invalid storedAs settings: File descriptor name doesn't match with proto file name")
    }

  }
}
