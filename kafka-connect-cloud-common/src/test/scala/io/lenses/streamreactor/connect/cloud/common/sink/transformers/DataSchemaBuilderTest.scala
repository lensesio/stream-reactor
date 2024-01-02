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
package io.lenses.streamreactor.connect.cloud.common.sink.transformers

import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.formats.writer.BooleanSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.IntSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.connect.data.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class DataSchemaBuilderTest extends AnyFunSuite with Matchers {
  test("create envelope schema containing Key, Values, Headers, and Metadata") {
    val storageSettings =
      DataStorageSettings(envelope = true, value = true, key = true, metadata = true, headers = true)

    val ts = Instant.now()
    val message = MessageDetail(
      StringSinkData("key", Some(Schema.STRING_SCHEMA)),
      StructSinkData(SampleData.Users.head),
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
    val actual = EnvelopeWithSchemaTransformer.envelopeSchema(message, storageSettings)
    actual.field("key").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    val valueSchema = actual.field("value").schema()
    valueSchema.field("name").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    valueSchema.field("title").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    valueSchema.field("salary").schema().`type`() shouldBe Schema.FLOAT64_SCHEMA.`type`()

    val headersSchema = actual.field("headers").schema()
    headersSchema.field("headerKey").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    headersSchema.field("headerKey2").schema().`type`() shouldBe Schema.BOOLEAN_SCHEMA.`type`()
    headersSchema.field("headerKey3").schema().`type`() shouldBe Schema.INT32_SCHEMA.`type`()

    val metadataSchema = actual.field("metadata").schema()
    metadataSchema.field("timestamp").schema().`type`() shouldBe Schema.INT64_SCHEMA.`type`()
    metadataSchema.field("topic").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    metadataSchema.field("partition").schema().`type`() shouldBe Schema.INT32_SCHEMA.`type`()
    metadataSchema.field("offset").schema().`type`() shouldBe Schema.INT64_SCHEMA.`type`()

  }

  test("create envelope schema containing Key, Values and Metadata") {
    val storageSettings =
      DataStorageSettings(envelope = true, value = true, key = true, metadata = true, headers = false)
    val ts = Instant.now()
    val message = MessageDetail(
      StringSinkData("key", Some(Schema.STRING_SCHEMA)),
      StructSinkData(SampleData.Users.head),
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
    val actual = EnvelopeWithSchemaTransformer.envelopeSchema(message, storageSettings)

    actual.field("key").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    val valueSchema = actual.field("value").schema()
    valueSchema.field("name").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    valueSchema.field("title").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    valueSchema.field("salary").schema().`type`() shouldBe Schema.FLOAT64_SCHEMA.`type`()

    actual.field("headers") shouldBe null
    val metadataSchema = actual.field("metadata").schema()
    metadataSchema.field("timestamp").schema().`type`() shouldBe Schema.INT64_SCHEMA.`type`()
    metadataSchema.field("topic").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    metadataSchema.field("partition").schema().`type`() shouldBe Schema.INT32_SCHEMA.`type`()
    metadataSchema.field("offset").schema().`type`() shouldBe Schema.INT64_SCHEMA.`type`()

  }

  test("create envelope schema containing  Values and Metadata") {
    val storageSettings =
      DataStorageSettings(key = false, metadata = true, headers = false, envelope = true, value = true)
    val ts = Instant.now()
    val message = MessageDetail(
      StringSinkData("key", Some(Schema.STRING_SCHEMA)),
      StructSinkData(SampleData.Users.head),
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
    val actual = EnvelopeWithSchemaTransformer.envelopeSchema(message, storageSettings)
    actual.field("key") shouldBe null
    val valueSchema = actual.field("value").schema()
    valueSchema.field("name").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    valueSchema.field("title").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    valueSchema.field("salary").schema().`type`() shouldBe Schema.FLOAT64_SCHEMA.`type`()

    actual.field("headers") shouldBe null

    val metadataSchema = actual.field("metadata").schema()
    metadataSchema.field("timestamp").schema().`type`() shouldBe Schema.INT64_SCHEMA.`type`()
    metadataSchema.field("topic").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    metadataSchema.field("partition").schema().`type`() shouldBe Schema.INT32_SCHEMA.`type`()
    metadataSchema.field("offset").schema().`type`() shouldBe Schema.INT64_SCHEMA.`type`()

  }
}
