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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import com.github.mustachejava.Mustache
import org.apache.kafka.connect.sink.SinkRecord

import java.io.StringWriter

class TemplateTest extends AnyFunSuite with Matchers {

  test("Template.process should execute endpoint and content templates") {

    val endpointResult   = TestStringWriter("endpoint_result")
    val endpointTemplate = mock(classOf[Mustache])
    when(endpointTemplate.execute(any[StringWriter](), any[Object]())).thenReturn(endpointResult)

    val contentResult   = TestStringWriter("content_result")
    val contentTemplate = mock(classOf[Mustache])
    when(contentTemplate.execute(any[StringWriter](), any[Object]())).thenReturn(contentResult)

    val headerTemplates = Seq[(Mustache, Mustache)]()
    val template        = Template(endpointTemplate, contentTemplate, headerTemplates)

    val sinkRecord = mock(classOf[SinkRecord])

    val processedTemplate = template.process(sinkRecord)

    processedTemplate.endpoint shouldBe endpointResult.toString
    processedTemplate.content shouldBe contentResult.toString
    processedTemplate.headers shouldBe Seq()

    when(endpointTemplate.execute(any[StringWriter](), any[Object]())).thenReturn(endpointResult)
    when(contentTemplate.execute(any[StringWriter](), any[Object]())).thenReturn(contentResult)

  }

  test("Template.executeTemplate should execute the given Mustache template") {
    val expectedResult   = TestStringWriter("template_result")
    val mustacheTemplate = mock(classOf[Mustache])
    when(mustacheTemplate.execute(any[StringWriter](), any[Object]())).thenReturn(expectedResult)

    val emptyHeaderTemplates = Seq[(Mustache, Mustache)]()
    val template             = Template(mustacheTemplate, mustacheTemplate, emptyHeaderTemplates)

    val sinkRecord = mock(classOf[SinkRecord])

    val result = template.process(sinkRecord)

    result.endpoint shouldBe expectedResult.toString
    result.content shouldBe expectedResult.toString
    result.headers shouldBe Seq()

    verify(mustacheTemplate, times(2)).execute(any[StringWriter](), any[Object]())
  }

  test("Template.executeTemplate should execute header Mustache template") {

    val expectedResult = TestStringWriter("template_result")

    val mustacheTemplate = mock(classOf[Mustache])
    when(mustacheTemplate.execute(any[StringWriter](), any[Object]())).thenReturn(expectedResult)

    val header1Key = mock(classOf[Mustache])
    when(header1Key.execute(any[StringWriter](), any[Object]())).thenReturn(TestStringWriter("header1Key"))

    val header1Value = mock(classOf[Mustache])
    when(header1Value.execute(any[StringWriter](), any[Object]())).thenReturn(TestStringWriter("header1Value"))

    val header2Key = mock(classOf[Mustache])
    when(header2Key.execute(any[StringWriter](), any[Object]())).thenReturn(TestStringWriter("header2Key"))

    val header2Value = mock(classOf[Mustache])
    when(header2Value.execute(any[StringWriter](), any[Object]())).thenReturn(TestStringWriter("header2Value"))

    val headerTemplates = Seq(
      header1Key -> header1Value,
      header2Key -> header2Value,
    )
    val template = Template(mustacheTemplate, mustacheTemplate, headerTemplates)

    val sinkRecord = mock(classOf[SinkRecord])

    val result = template.process(sinkRecord)

    result.headers shouldBe Seq(
      "header1Key" -> "header1Value",
      "header2Key" -> "header2Value",
    )

    verify(mustacheTemplate, times(2)).execute(any[StringWriter](), any[Object]())
    verify(header1Key).execute(any[StringWriter](), any[Object]())
    verify(header1Value).execute(any[StringWriter](), any[Object]())
    verify(header2Key).execute(any[StringWriter](), any[Object]())
    verify(header2Value).execute(any[StringWriter](), any[Object]())
  }
}
