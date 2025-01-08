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
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.connect.data.Schema
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

class TextConverterTest extends AnyFunSuite with Matchers {
  implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val location     = CloudLocation("bucket", "prefix".some, "a/b/c.txt".some)
  private val lastModified = Instant.ofEpochMilli(1000)

  test("convert to SourceRecord setting the value and value schema with watermark headers") {

    val actual =
      new TextConverter(true, Map("a" -> "1").asJava, Topic("topic1"), 1, location, lastModified).convert("value",
                                                                                                          1,
                                                                                                          lastLine =
                                                                                                            true,
      )

    actual.valueSchema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
    actual.value() shouldBe "value"
    actual.topic() shouldBe "topic1"
    actual.kafkaPartition() shouldBe 1
    actual.sourcePartition() shouldBe Map("a" -> "1").asJava
    actual.sourceOffset() shouldBe Map("line" -> "1", "path" -> "a/b/c.txt", "ts" -> "1000", "last" -> "t").asJava
    actual.key() shouldBe null
    actual.keySchema() shouldBe null
    actual.headers().size() shouldBe 5
    actual.headers().iterator().asScala.map(hdr =>
      (hdr.key(), hdr.value().asInstanceOf[String]),
    ).toMap should contain allOf (
      ("path", "a/b/c.txt"),
      ("line", "1"),
      ("ts", "1000"),
      ("a", "1"),
      ("last", "t"),
    )
  }

  test("convert to SourceRecord without injecting watermark headers") {

    val actual =
      new TextConverter(false, Map("a" -> "1").asJava, Topic("topic1"), 1, location, lastModified).convert("value",
                                                                                                           1,
                                                                                                           lastLine =
                                                                                                             true,
      )

    actual.headers() shouldBe empty
  }
}
