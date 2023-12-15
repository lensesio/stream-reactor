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
package io.lenses.streamreactor.connect.http.sink.tpl.templates

import com.github.mustachejava.Mustache
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RawTemplateTest extends AnyFunSuite with Matchers {

  test("RawTemplate.parse should create a Template with Mustache templates") {
    val endpoint = "Endpoint: {{key}}"
    val content  = "Content: {{value}}"
    val headers  = Seq(("HeaderKey1", "HeaderValue1"), ("HeaderKey2", "HeaderValue2"))

    val rawTemplate = RawTemplate(endpoint, content, headers)
    val template    = rawTemplate.parse()

    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.key()).thenReturn("KeyData")
    when(sinkRecord.value()).thenReturn("ValueData")

    val endpointResult = "Endpoint: KeyData"
    val contentResult  = "Content: ValueData"

    val headerResults = Seq("HeaderKey1" -> "HeaderValue1", "HeaderKey2" -> "HeaderValue2")

    val processedTemplate = template.process(sinkRecord)

    processedTemplate.endpoint shouldBe endpointResult
    processedTemplate.content shouldBe contentResult
    processedTemplate.headers shouldBe headerResults
  }

  test("RawTemplate.parse should create Mustache templates for headers") {
    val endpoint = "Endpoint: {{key}}"
    val content  = "Content: {{value}}"
    val headers  = Seq(("HeaderKey1", "HeaderValue1"), ("HeaderKey2", "HeaderValue2"))

    val rawTemplate = RawTemplate(endpoint, content, headers)
    val template    = rawTemplate.parse()

    template.headerTemplates.foreach {
      case (keyTemplate, valueTemplate) =>
        keyTemplate shouldBe a[Mustache]
        valueTemplate shouldBe a[Mustache]
    }
  }

  test("RawTemplate.parse should correctly execute header Mustache templates") {
    val endpoint = "Endpoint: {{key}}"
    val content  = "Content: {{value}}"
    val headers  = Seq(("HeaderKey1", "HeaderValue1"), ("HeaderKey2", "HeaderValue2"))

    val rawTemplate = RawTemplate(endpoint, content, headers)
    val template    = rawTemplate.parse()

    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.key()).thenReturn("KeyData")
    when(sinkRecord.value()).thenReturn("ValueData")

    val headerResults = Seq("HeaderKey1" -> "HeaderValue1", "HeaderKey2" -> "HeaderValue2")

    val processedTemplate = template.process(sinkRecord)

    processedTemplate.headers shouldBe headerResults
  }
}
