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
package io.lenses.streamreactor.connect.http.sink.tpl

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.when
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RawTemplateTest extends AnyFunSuite with Matchers with EitherValues with LazyLogging {

  test("parse should process templates") {

    val template = RawTemplate(
      endpoint = "Endpoint: {{key}}",
      content  = "Content: {{value}}",
      headers = Seq(
        ("HeaderKey1", "HeaderValue1"),
        ("HeaderKey2", "HeaderValue2"),
      ),
    )

    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.key()).thenReturn("KeyData")
    when(sinkRecord.value()).thenReturn("ValueData")

    val rendered = template.renderRecords(Seq(sinkRecord))

    val processedTemplate = template.process(rendered.value)
    processedTemplate.value.endpoint should be("Endpoint: KeyData")
    processedTemplate.value.content should be("Content: ValueData")
    processedTemplate.value.headers should be(
      Seq("HeaderKey1" -> "HeaderValue1", "HeaderKey2" -> "HeaderValue2"),
    )
  }

  test("parse should correctly execute header templates") {
    val template = RawTemplate(
      endpoint = "Endpoint: {{key}}",
      content  = "Content: {{value}}",
      headers = Seq(
        ("HeaderKey1-{{topic}}", "HeaderValue1-{{topic}}"),
        ("HeaderKey2-{{partition}}", "HeaderValue2-{{partition}}"),
      ),
    )

    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.key()).thenReturn("KeyData")
    when(sinkRecord.value()).thenReturn("ValueData")
    when(sinkRecord.topic()).thenReturn("myTopic")
    when(sinkRecord.kafkaPartition()).thenReturn(100)

    val headerResults = Seq(
      "HeaderKey1-myTopic" -> "HeaderValue1-myTopic",
      "HeaderKey2-100"     -> "HeaderValue2-100",
    )

    val rendered = template.renderRecords(Seq(sinkRecord))

    val processedTemplate = template.process(rendered.value)
    processedTemplate.value.headers shouldBe headerResults
  }

}
