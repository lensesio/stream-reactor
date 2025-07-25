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
package io.lenses.streamreactor.connect.cassandra.adapters

import com.datastax.oss.common.sink.record.RawData
import com.datastax.oss.common.sink.record.StructData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala

class StructAdapterTest extends AnyFunSuite with Matchers {

  private val schema =
    SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("bytes", Schema.BYTES_SCHEMA)
      .build()

  private val bytesArray = Array[Byte](3, 2, 1)
  private val struct = new Struct(schema)
    .put("bigint", 1234L)
    .put("boolean", false)
    .put("bytes", bytesArray)
  private val structData = new StructData(StructAdapter(struct))

  test("should parse field names from struct") {
    structData.fields().asScala should contain theSameElementsAs (RawData.FIELD_NAME +: Seq("bigint",
                                                                                            "boolean",
                                                                                            "bytes",
    ))
  }

  test("should get field value") {
    structData.getFieldValue("bigint") shouldBe 1234L
    structData.getFieldValue("boolean") shouldBe false
    structData.getFieldValue("bytes") shouldBe ByteBuffer.wrap(bytesArray)
  }

  test("should handle null struct") {
    val empty = new StructData(null)
    empty.fields().asScala should contain only RawData.FIELD_NAME
    empty.getFieldValue("junk") shouldBe null
  }
}
