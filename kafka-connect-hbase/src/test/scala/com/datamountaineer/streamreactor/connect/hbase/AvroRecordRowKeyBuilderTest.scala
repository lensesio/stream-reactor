/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.hbase

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import com.datamountaineer.streamreactor.connect.hbase.avro.AvroRecordFieldExtractorMapFn
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class AvroRecordRowKeyBuilderTest extends WordSpec with Matchers with MockitoSugar {
  val schema: Schema = new Schema.Parser().parse(PersonAvroSchema.schema)

  "AvroRecordRowKeyBuilder" should {
    "extract the values from the avro record and create the key" in {
      val keys = Seq("firstName", "lastName", "age")
      val rowKeyBuilder = new AvroRecordRowKeyBuilderBytes(AvroRecordFieldExtractorMapFn(schema, keys), keys)

      val sinkRecord = mock[SinkRecord]
      val firstName = "Jack"
      val lastName = "Smith"
      val age = 29

      val record = new GenericRecord {

        val values: Map[String, AnyRef] = Map("firstName" -> firstName, "lastName" -> lastName, "age" -> Int.box(age))

        override def get(key: String): AnyRef = values(key)

        override def put(key: String, v: scala.Any): Unit = sys.error("not supported")

        override def get(i: Int): AnyRef = sys.error("not supported")


        override def put(i: Int, v: scala.Any): Unit = sys.error("not supported")


        override def getSchema: Schema = sys.error("not supported")
      }

      val expectedValue = Bytes.add(
        Array(
          firstName.fromString(),
          rowKeyBuilder.delimBytes,
          lastName.fromString(),
          rowKeyBuilder.delimBytes,
          age.fromInt()))
      rowKeyBuilder.build(sinkRecord, record) shouldBe expectedValue
    }
  }
}
