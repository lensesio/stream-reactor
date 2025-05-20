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
package io.lenses.streamreactor.connect.cloud.common.formats.reader.converters

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.formats.bytes.BytesOutputRow
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.connect.data.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava

class BytesOutputRowConverterTest extends AnyFunSuite with Matchers {

  implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  test("convert to SourceRecord and populate the key and value") {
    val location     = CloudLocation("bucket", "myprefix".some, "a/b/c.txt".some)
    val lastModified = Instant.ofEpochMilli(10001)

    val valueBytes = "value".getBytes

    val actual = new BytesOutputRowConverter(
      true,
      Map("a" -> "1").asJava,
      Topic("topic1"),
      1,
      location,
      lastModified,
    ).convert(BytesOutputRow(
                valueBytes,
              ),
              2,
              lastLine = true,
    )
    actual.key() shouldBe null
    actual.value() shouldBe "value".getBytes
    actual.topic() shouldBe "topic1"
    actual.kafkaPartition() shouldBe 1
    actual.sourcePartition() shouldBe Map("a" -> "1").asJava
    actual.sourceOffset() shouldBe Map("line" -> "2", "path" -> "a/b/c.txt", "ts" -> "10001", "last" -> "t").asJava
    actual.keySchema().`type`() shouldBe Schema.BYTES_SCHEMA.`type`()
    actual.valueSchema().`type`() shouldBe Schema.BYTES_SCHEMA.`type`()
  }
}
