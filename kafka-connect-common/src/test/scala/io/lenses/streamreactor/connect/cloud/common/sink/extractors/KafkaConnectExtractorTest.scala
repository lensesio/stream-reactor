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
package io.lenses.streamreactor.connect.cloud.common.sink.extractors

import cats.implicits.catsSyntaxOptionId
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.EitherValues

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class KafkaConnectExtractorTest extends AnyFlatSpec with Matchers with MockitoSugar with EitherValues {

  "extractFromKey" should "return the correct value for a given key" in {
    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.key()).thenReturn("testKey")
    when(sinkRecord.keySchema()).thenReturn(null)

    val result = KafkaConnectExtractor.extractFromKey(sinkRecord, None)
    result shouldBe Right("testKey")
  }

  "extractFromValue" should "return the correct value for a given value" in {
    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.value()).thenReturn("testValue")
    when(sinkRecord.valueSchema()).thenReturn(null)

    val result = KafkaConnectExtractor.extractFromValue(sinkRecord, None)
    result shouldBe Right("testValue")
  }

  "extract" should "handle different types correctly" in {
    KafkaConnectExtractor.extract(123: java.lang.Integer, None, None) shouldBe Right(123)
    KafkaConnectExtractor.extract(123L: java.lang.Long, None, None) shouldBe Right(123L)
    KafkaConnectExtractor.extract(123.45: java.lang.Double, None, None) shouldBe Right(123.45)
    KafkaConnectExtractor.extract(true: java.lang.Boolean, None, None) shouldBe Right(true)
    KafkaConnectExtractor.extract("testString", None, None) shouldBe Right("testString")
  }

  it should "handle Struct type correctly" in {

    val fieldName  = "field1"
    val fieldValue = "fieldValue"

    val structSchema = SchemaBuilder.struct().field(fieldName, Schema.STRING_SCHEMA).build()

    val struct = new Struct(structSchema).put(fieldName, fieldValue)

    val result = KafkaConnectExtractor.extract(struct, None, Some(fieldName))
    result shouldBe Right(fieldValue)
  }

  it should "handle Map type correctly" in {
    val map = Map("key1" -> "value1").asJava

    val partitionNamePath = "key1"
    val result            = KafkaConnectExtractor.extract(map, None, Some(partitionNamePath))
    result shouldBe Right("value1")
  }

  it should "handle List type correctly" in {
    val list              = List("value1", "value2").asJava
    val partitionNamePath = "0"

    val result = KafkaConnectExtractor.extract(list,
                                               SchemaBuilder.array(Schema.STRING_SCHEMA).build().some,
                                               Some(partitionNamePath),
    )
    result shouldBe Right("value1")
  }

  it should "return an error for unknown types" in {
    val unknownType = new Object()
    val result      = KafkaConnectExtractor.extract(unknownType, None, None)
    val message     = result.left.value.getMessage
    message should startWith("Unknown value type: `java.lang.Object`")
    message should endWith("path: `Empty`")
  }

  it should "return an error for null types" in {
    val result = KafkaConnectExtractor.extract(null, None, None)
    result.value should be(null)
  }
}
