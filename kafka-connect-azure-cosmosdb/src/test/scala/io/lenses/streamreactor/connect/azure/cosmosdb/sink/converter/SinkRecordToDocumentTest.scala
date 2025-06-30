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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter

import cats.implicits._
import com.azure.cosmos.implementation.Document
import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.azure.cosmosdb.Json
import io.lenses.streamreactor.connect.azure.cosmosdb.config.KeySource
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.Transaction
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues
import scala.annotation.nowarn
import scala.util.Using

@nowarn
class SinkRecordToDocumentTest
    extends AnyWordSpec
    with Matchers
    with ConverterUtil
    with MockitoSugar
    with ArgumentMatchersSugar
    with EitherValues {
  private val testId = "myTestId"
  private val idGenerator: KeySource = mock[KeySource]
  when(idGenerator.generateId(any[SinkRecord])).thenReturn(testId.asRight)

  "SinkRecordToDocument" should {
    "convert Kafka Struct to a Azure Document Db Document" in {
      for (i <- 1 to 4) {
        val json = Using(scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath))(
          _.mkString,
        ).getOrElse(fail("resource not found"))
        val tx     = Json.fromJson[Transaction](json)
        val record = new SinkRecord("topic1", 0, null, null, Transaction.ConnectSchema, tx.toStruct, 0)

        val fields        = Map.empty[String, String]
        val ignoredFields = Set.empty[String]
        val document      = SinkRecordToDocument(record, fields, ignoredFields, idGenerator)
        val expected      = new Document(json).setId(testId)

        //comparing string representation; we have more specific types given the schema
        document.value.toString shouldBe expected.toString
      }
    }

    "convert String Schema + Json payload to a Azure Document DB Document" in {
      for (i <- 1 to 4) {
        val json = Using(scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath))(
          _.mkString,
        ).getOrElse(fail("resource not found"))

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        val fields        = Map.empty[String, String]
        val ignoredFields = Set.empty[String]
        val document      = SinkRecordToDocument(record, fields, ignoredFields, idGenerator)
        val expected      = new Document(json).setId(testId)

        //comparing string representation; we have more specific types given the schema
        document.value.toString shouldBe expected.toString
      }
    }

    "convert Schemaless + Json payload to a Azure Document DB Document" in {
      for (i <- 1 to 4) {
        val json = Using(scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath))(
          _.mkString,
        ).getOrElse(fail("resource not found"))

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        val fields        = Map.empty[String, String]
        val ignoredFields = Set.empty[String]
        val document      = SinkRecordToDocument(record, fields, ignoredFields, idGenerator)
        val expected      = new Document(json).setId(testId)

        //comparing string representation; we have more specific types given the schema
        document.value.toString shouldBe expected.toString
      }
    }
  }
}
