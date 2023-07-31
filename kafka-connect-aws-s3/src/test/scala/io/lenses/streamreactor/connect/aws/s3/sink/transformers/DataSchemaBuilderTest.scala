/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.sink.transformers

import io.lenses.streamreactor.connect.aws.s3.config.DataStorageSettings
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class DataSchemaBuilderTest extends AnyFunSuite with Matchers {
  test("create envelope schema containing Key, Values, Headers, and Metadata") {
    val storageSettings = DataStorageSettings(keys = true, metadata = true, headers = true)

    val ts = Instant.now()
    val message = MessageDetail(
      Some(StringSinkData("key", Some(Schema.STRING_SCHEMA))),
      StructSinkData(TestSampleSchemaAndData.users.head),
      Map(
        "headerKey"  -> StringSinkData("headerValue", Some(Schema.STRING_SCHEMA)),
        "headerKey2" -> BooleanSinkData(true, Some(Schema.BOOLEAN_SCHEMA)),
        "headerKey3" -> IntSinkData(3, Some(Schema.INT32_SCHEMA)),
      ),
      Some(ts),
      Topic("topic"),
      1,
      Offset(2),
    )
    val actual = MessageTransformer.envelopeSchema(message, storageSettings)
    val expected = SchemaBuilder.struct()
      .field("key", Schema.STRING_SCHEMA).optional()
      .field("value", TestSampleSchemaAndData.users.head.schema).optional()
      .field(
        "headers",
        SchemaBuilder.struct()
          .field("headerKey", Schema.STRING_SCHEMA)
          .field("headerKey2", Schema.BOOLEAN_SCHEMA)
          .field("headerKey3", Schema.INT32_SCHEMA)
          .build(),
      )
      .field(
        "metadata",
        SchemaBuilder.struct()
          .field("timestamp", Schema.INT64_SCHEMA).optional()
          .field("topic", Schema.STRING_SCHEMA)
          .field("partition", Schema.INT32_SCHEMA)
          .field("offset", Schema.INT64_SCHEMA)
          .build(),
      ).build()

    actual shouldBe expected
  }

  test("create envelope schema containing Key, Values and Metadata") {
    val storageSettings = DataStorageSettings(keys = true, metadata = true, headers = false)
    val ts              = Instant.now()
    val message = MessageDetail(
      Some(StringSinkData("key", Some(Schema.STRING_SCHEMA))),
      StructSinkData(TestSampleSchemaAndData.users.head),
      Map(
        "headerKey"  -> StringSinkData("headerValue", Some(Schema.STRING_SCHEMA)),
        "headerKey2" -> BooleanSinkData(true, Some(Schema.BOOLEAN_SCHEMA)),
        "headerKey3" -> IntSinkData(3, Some(Schema.INT32_SCHEMA)),
      ),
      Some(ts),
      Topic("topic"),
      1,
      Offset(2),
    )
    val actual = MessageTransformer.envelopeSchema(message, storageSettings)
    val expected = SchemaBuilder.struct()
      .field("key", Schema.STRING_SCHEMA).optional()
      .field("value", TestSampleSchemaAndData.users.head.schema).optional()
      .field(
        "metadata",
        SchemaBuilder.struct()
          .field("timestamp", Schema.INT64_SCHEMA).optional()
          .field("topic", Schema.STRING_SCHEMA)
          .field("partition", Schema.INT32_SCHEMA)
          .field("offset", Schema.INT64_SCHEMA)
          .build(),
      ).build()

    actual shouldBe expected
  }

  test("no envelope returns the value schema") {
    val storageSettings = DataStorageSettings(keys = false, metadata = false, headers = false)
    val ts              = Instant.now()
    val message = MessageDetail(
      Some(StringSinkData("key", Some(Schema.STRING_SCHEMA))),
      StructSinkData(TestSampleSchemaAndData.users.head),
      Map(
        "headerKey"  -> StringSinkData("headerValue", Some(Schema.STRING_SCHEMA)),
        "headerKey2" -> BooleanSinkData(true, Some(Schema.BOOLEAN_SCHEMA)),
        "headerKey3" -> IntSinkData(3, Some(Schema.INT32_SCHEMA)),
      ),
      Some(ts),
      Topic("topic"),
      1,
      Offset(2),
    )
    val actual   = MessageTransformer.envelopeSchema(message, storageSettings)
    val expected = TestSampleSchemaAndData.users.head.schema

    actual shouldBe expected
  }
  test("create envelope schema containing  Values and Metadata") {
    val storageSettings = DataStorageSettings(keys = false, metadata = true, headers = false)
    val ts              = Instant.now()
    val message = MessageDetail(
      Some(StringSinkData("key", Some(Schema.STRING_SCHEMA))),
      StructSinkData(TestSampleSchemaAndData.users.head),
      Map(
        "headerKey"  -> StringSinkData("headerValue", Some(Schema.STRING_SCHEMA)),
        "headerKey2" -> BooleanSinkData(true, Some(Schema.BOOLEAN_SCHEMA)),
        "headerKey3" -> IntSinkData(3, Some(Schema.INT32_SCHEMA)),
      ),
      Some(ts),
      Topic("topic"),
      1,
      Offset(2),
    )
    val actual = MessageTransformer.envelopeSchema(message, storageSettings)
    val expected = SchemaBuilder.struct()
      .field("value", TestSampleSchemaAndData.users.head.schema).optional()
      .field(
        "metadata",
        SchemaBuilder.struct()
          .field("timestamp", Schema.INT64_SCHEMA).optional()
          .field("topic", Schema.STRING_SCHEMA)
          .field("partition", Schema.INT32_SCHEMA)
          .field("offset", Schema.INT64_SCHEMA)
          .build(),
      ).build()

    actual shouldBe expected
  }
}
