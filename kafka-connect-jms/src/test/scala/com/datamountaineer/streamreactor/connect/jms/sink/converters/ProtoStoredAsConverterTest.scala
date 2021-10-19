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

import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSetting, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.{TestBase, Using}
import org.apache.kafka.connect.data.{Schema, Struct}
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
    val kcql = getKCQLStoreAsTimedPerson(queueName, kafkaTopic1, "QUEUE", getProtoPath)
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchemaTimestamp
    val struct = getProtobufStructTimestamp(schema, "non-addressed-person", 101, "1970-01-01T00:00:00Z")
    val (setting: JMSSetting, record: SinkRecord) = getRecordAndSetting(converter, kafkaTopic1, props, schema, struct)

    val convertedValue: Array[Byte] = converter.convert(record, setting)

    assertTimedPersonDetails(convertedValue, "non-addressed-person", 101, "1970-01-01T00:00:00Z")

  }

  "create a BytesMessage with sinkrecord payload with connector config proto path properties" in {
    val converter = ProtoStoredAsConverter()
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val path = getProtoPath
    val kcql = getKCQLEmptyStoredAsNonAddressedPerson(queueName, kafkaTopic1, "QUEUE")
    val props = getProps(kcql, JMS_URL) ++
      Map("connect.sink.converter.proto_path" -> path)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "non-addrressed-person", 102, "non-addressed-person@gmail.com")
    val (setting: JMSSetting, record: SinkRecord) = getRecordAndSetting(converter, kafkaTopic1, props, schema, struct)

    val convertedValue: Array[Byte] = converter.convert(record, setting)

    assertPersonDetails(convertedValue, "non-addressed-person", 102, "non-addressed-person@gmail.com")
  }

  "create a BytesMessage with sinkrecord payload with only storedAs Name" in {
    val converter = ProtoStoredAsConverter()
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQLStoredAsWithNameOnly(queueName, kafkaTopic1, "QUEUE")
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val (setting: JMSSetting, record: SinkRecord) = getRecordAndSetting(converter, kafkaTopic1, props, schema, struct)

    val convertedValue: Array[Byte] = converter.convert(record, setting)

    assertPersonDetails(convertedValue, "addressed-person", 103, "addressed-person@gmail.com")
  }

  "should throw exception for invalid value for storedAs" in {
    val converter = ProtoStoredAsConverter()
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQLStoredAsWithInvalidData(queueName, kafkaTopic1, "QUEUE")
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val (setting: JMSSetting, record: SinkRecord) = getRecordAndSetting(converter, kafkaTopic1, props, schema, struct)

    val caught =
      intercept[DataException] {
        converter.convert(record, setting)
      }

    assert(caught.getMessage == "Invalid storedAs settings: NonAddressedPersonOuterClass")
  }

  "should throw exception for invalid package name for storedAs when protopath is present" in {
    val converter = ProtoStoredAsConverter()
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQLStoredAsWithInvalidPackageNameWithProtopath(queueName, kafkaTopic1, "QUEUE", getProtoPath)
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val (setting: JMSSetting, record: SinkRecord) = getRecordAndSetting(converter, kafkaTopic1, props, schema, struct)

    val caught =
      intercept[DataException] {
        converter.convert(record, setting)
      }

    assert(caught.getMessage == "Invalid storedAs settings: Proto file package name doesn't match with storedAs package name")
  }

  "should throw exception for valid package name for storedAs but invalid protopath which has no files in it" in {
    val converter = ProtoStoredAsConverter()
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQLStoredAsWithProtopath(queueName, kafkaTopic1, "QUEUE", "/resources/path")
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val (setting: JMSSetting, record: SinkRecord) = getRecordAndSetting(converter, kafkaTopic1, props, schema, struct)

    val caught =
      intercept[DataException] {
        converter.convert(record, setting)
      }

    assert(caught.getMessage == "Invalid storedAs settings: /resources/path")
  }

  "should throw exception for for incorrect proto file name" in {
    val converter = ProtoStoredAsConverter()
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQLStoreAsWithFileAndPath(queueName, kafkaTopic1, "QUEUE", "`NonExisting.proto`", getProtoPath)
    val props = getProps(kcql, JMS_URL)
    val schema = getProtobufSchema
    val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
    val (setting: JMSSetting, record: SinkRecord) = getRecordAndSetting(converter, kafkaTopic1, props, schema, struct)

    val caught =
      intercept[DataException] {
        converter.convert(record, setting)
      }

    assert(caught.getMessage == "Invalid storedAs settings: File descriptor name=datamountaineer.streamreactor.example.NonAddressedPerson doesn't match with proto file name=NonExisting.proto")
  }

  private def assertTimedPersonDetails(convertedValue: Array[Byte], name: String, id: Int, time: String) = {
    val stringValue = convertedValue.map(_.toChar).mkString
    Option(stringValue).isDefined shouldBe true
    stringValue.contains("name") shouldBe true
    stringValue.contains(name) shouldBe true
    stringValue.contains("id") shouldBe true
    stringValue.contains(id) shouldBe true
    stringValue.contains("timestamp") shouldBe true
    stringValue.contains(time) shouldBe true
  }

  private def getRecordAndSetting(converter: ProtoStoredAsConverter, kafkaTopic1: String, props: Map[String, String], schema: Schema, struct: Struct) = {
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, true)
    val setting = settings.settings.head

    converter.initialize(props.asJava)
    val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)
    (setting, record)
  }

  private def assertPersonDetails(convertedValue: Array[Byte], name: String, id: Int, email: String) = {
    val stringValue = convertedValue.map(_.toChar).mkString
    Option(stringValue).isDefined shouldBe true
    stringValue.contains("name") shouldBe true
    stringValue.contains(name) shouldBe true
    stringValue.contains("id") shouldBe true
    stringValue.contains("email") shouldBe true
    stringValue.contains(email) shouldBe true
  }

  private def getProtoPath = {
    getClass.getClassLoader
      .getResource("proto/AddressedPerson.proto")
      .getPath
      .replace("AddressedPerson.proto", "")
  }
}
