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
package io.lenses.streamreactor.connect.http.sink.tpl.renderer

import enumeratum.CirceEnum
import enumeratum.Enum
import io.lenses.streamreactor.connect.http.sink.tpl.RawTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class TemplateRendererTest extends AnyFunSuiteLike with Matchers with EitherValues {

  private val templateRenderer = new TemplateRenderer(SubstitutionType)
  private val testSinkRecord   = new SinkRecord("topic", 0, null, null, null, null, 0)
  private val testValue        = "testValue"

  test("renderRecords should tidy json commas if set") {

    val record1 = new SinkRecord("myTopic", 0, null, null, Schema.STRING_SCHEMA, "\"m1\"", 9)
    val record2 = new SinkRecord("myTopic", 0, null, null, Schema.STRING_SCHEMA, "\"m2\"", 10)
    val record3 = new SinkRecord("myTopic", 0, null, null, Schema.STRING_SCHEMA, "\"m3\"", 10)

    val records = Seq(record1, record2, record3)

    val processedTemplate = RawTemplate(
      endpoint = "http://www.example.com",
      content  = "{\"data\":[{{#message}}{{value}},{{/message}}]}",
      Seq(),
    )

    val rendered = processedTemplate.renderRecords(records)

    val processed = processedTemplate.process(rendered.value, tidyJson = true)
    normalized(processed.value.content) should be(
      normalized(
        """{"data":["m1","m2","m3"]}""".stripMargin,
      ),
    )
  }

  private def normalized(s: String): String =
    s
      .replaceAll(">\\s+<", "><")
      .replaceAll("(?s)\\s+", " ").trim

  test("getValue should return SubstitutionError when tag is null") {
    val result = templateRenderer.getTagValueFromData(null, testSinkRecord)
    result.left.value.msg shouldBe "No tag specified"
  }

  test("getValue should handle valid tags correctly") {
    val result = templateRenderer.getTagValueFromData("#message", testSinkRecord)
    result shouldBe Right("")
  }

  test("getValue should return SubstitutionError for unknown substitution type") {
    val result  = templateRenderer.getTagValueFromData("unknownType", testSinkRecord)
    val leftVal = result.left.value
    leftVal.msg should be("Couldn't find `unknowntype` SubstitutionType")
  }

  case object TestSubstitutionType extends SubstitutionType {
    override def get(locator: Option[String], sinkRecord: SinkRecord): Either[SubstitutionError, AnyRef] =
      locator match {
        case Some(_) => Right(testValue)
        case None    => Left(SubstitutionError("SubstitutionType returned null"))
      }
  }

  case object TestSubstitutionTypeEnum extends Enum[SubstitutionType] with CirceEnum[SubstitutionType] {
    override def values: IndexedSeq[SubstitutionType] =
      IndexedSeq(TestSubstitutionType)
  }

  test("getValue should handle test locator successfully") {
    val result =
      new TemplateRenderer(TestSubstitutionTypeEnum).getTagValueFromData("testsubstitutiontype.name", testSinkRecord)
    result.value shouldBe testValue
  }

  test("getValue should handle locator that returns null without throwing NPE") {
    val result =
      new TemplateRenderer(SubstitutionType).getTagValueFromData("value", testSinkRecord)
    result.value shouldBe ""
  }
}
