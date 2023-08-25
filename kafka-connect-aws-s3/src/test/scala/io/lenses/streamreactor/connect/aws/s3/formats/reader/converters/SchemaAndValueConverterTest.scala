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
package io.lenses.streamreactor.connect.aws.s3.formats.reader.converters

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava

class SchemaAndValueConverterTest extends AnyFunSuite with Matchers {

  test("converts a schema and value and populates the source record") {
    val value = new SchemaAndValue(
      Schema.OPTIONAL_STRING_SCHEMA,
      "lore ipsum",
    )
    val location     = S3Location("bucket", "prefix".some, "a/b/c.txt".some)
    val lastModified = Instant.ofEpochMilli(1000)
    val actual = new SchemaAndValueConverter(
      Map("a" -> "1").asJava,
      Topic("topic1"),
      1,
      location,
      lastModified,
    ).convert(value, 1)
    actual.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.value() shouldBe "value"
    actual.kafkaPartition() shouldBe 1
    actual.sourcePartition() shouldBe Map("a" -> "1").asJava
    actual.sourceOffset() shouldBe Map("line" -> "1", "path" -> "a/b/c.txt", "ts" -> "1000").asJava
    actual.key() shouldBe null
    actual.keySchema() shouldBe null
    actual.headers().size() shouldBe 0
  }
}
