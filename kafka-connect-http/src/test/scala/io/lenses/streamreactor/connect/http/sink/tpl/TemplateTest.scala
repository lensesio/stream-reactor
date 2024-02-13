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

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class TemplateTest extends AnyFunSuiteLike with Matchers with EitherValues {

  private val multiTemplate =
    """
      |<xml>
      |  <messages>
      |    {{#message}}
      |    <message>
      |      <topic>{{topic}}</topic>
      |      <employee>{{value.employeeId}}</employee>
      |      <order>{{value.orderNo}}</order>
      |      <groupDomain>{{value.groupDomain}}</groupDomain>
      |    </message>
      |    {{/message}}
      |  </messages>
      |</xml>""".stripMargin

  test("template behaviour") {

    val valueSchema = SchemaBuilder
      .struct()
      .name("myStruct")
      .field("groupDomain", Schema.STRING_SCHEMA)
      .field("orderNo", Schema.INT32_SCHEMA)
      .field("employeeId", Schema.STRING_SCHEMA)
      .build()

    val value = new Struct(valueSchema)
    value.put("groupDomain", "myExampleGroup.uk")
    value.put("orderNo", 10)
    value.put("employeeId", "Abcd1234")

    val record = new SinkRecord("myTopic", 0, null, null, valueSchema, value, 9)
    val processedTemplate = RawTemplate(
      endpoint = "http://{{value.groupDomain}}.example.com/{{value.orderNo}}/{{value.employeeId}}/{{topic}}",
      content =
        """<xml>
          |  <topic>{{topic}}</topic>
          |  <employee>{{value.employeeId}}</employee>
          |  <order>{{value.orderNo}}</order>
          |  <groupDomain>{{value.groupDomain}}</groupDomain>
          |</xml>""".stripMargin,
      Seq(),
    )

    val rendered = processedTemplate.renderRecords(Seq(record))

    val processed = processedTemplate.process(rendered.value)
    processed.value.endpoint should be("http://myExampleGroup.uk.example.com/10/Abcd1234/myTopic")

    processed.value.content should be(
      """<xml>
        |  <topic>myTopic</topic>
        |  <employee>Abcd1234</employee>
        |  <order>10</order>
        |  <groupDomain>myExampleGroup.uk</groupDomain>
        |</xml>""".stripMargin,
    )
  }

  test("template behaviour for batching messages") {

    val valueSchema = SchemaBuilder
      .struct()
      .name("myStruct")
      .field("groupDomain", Schema.STRING_SCHEMA)
      .field("orderNo", Schema.INT32_SCHEMA)
      .field("employeeId", Schema.STRING_SCHEMA)
      .build()

    val value1 = new Struct(valueSchema)
    value1.put("groupDomain", "myExampleGroup.uk")
    value1.put("orderNo", 10)
    value1.put("employeeId", "Abcd1234")

    val value2 = new Struct(valueSchema)
    value2.put("groupDomain", "myExampleGroup.uk")
    value2.put("orderNo", 11)
    value2.put("employeeId", "Efgh5678")

    val record1 = new SinkRecord("myTopic", 0, null, null, valueSchema, value1, 9)
    val record2 = new SinkRecord("myTopic", 0, null, null, valueSchema, value2, 10)

    val records = Seq(record1, record2)

    val processedTemplate = RawTemplate(
      endpoint = "http://{{value.groupDomain}}.example.com/{{value.orderNo}}/{{value.employeeId}}/{{topic}}",
      content  = multiTemplate,
      Seq(),
    )

    val rendered = processedTemplate.renderRecords(records)

    val processed = processedTemplate.process(rendered.value)
    processed.value.endpoint should be("http://myExampleGroup.uk.example.com/10/Abcd1234/myTopic")

    normalized(processed.value.content) should be(
      normalized(
        """
          |<xml>
          |  <messages>
          |    <message>
          |      <topic>myTopic</topic>
          |       <employee>Abcd1234</employee>
          |       <order>10</order>
          |       <groupDomain>myExampleGroup.uk</groupDomain>
          |    </message>
          |    <message>
          |       <topic>myTopic</topic>
          |       <employee>Efgh5678</employee>
          |       <order>11</order>
          |       <groupDomain>myExampleGroup.uk</groupDomain>
          |    </message>
          |  </messages>
          |</xml>""".stripMargin,
      ),
    )
  }

  test("template behaviour for batching messages without whitespace in between") {

    val valueSchema = SchemaBuilder
      .struct()
      .name("myStruct")
      .field("groupDomain", Schema.STRING_SCHEMA)
      .field("orderNo", Schema.INT32_SCHEMA)
      .field("employeeId", Schema.STRING_SCHEMA)
      .build()

    val value1 = new Struct(valueSchema)
    value1.put("groupDomain", "myExampleGroup.uk")
    value1.put("orderNo", 10)
    value1.put("employeeId", "Abcd1234")

    val value2 = new Struct(valueSchema)
    value2.put("groupDomain", "myExampleGroup.uk")
    value2.put("orderNo", 11)
    value2.put("employeeId", "Efgh5678")

    val record1 = new SinkRecord("myTopic", 0, null, null, valueSchema, value1, 9)
    val record2 = new SinkRecord("myTopic", 0, null, null, valueSchema, value2, 10)

    val records = Seq(record1, record2)

    val processedTemplate = RawTemplate(
      endpoint = "http://{{value.groupDomain}}.example.com/{{value.orderNo}}/{{value.employeeId}}/{{topic}}",
      content  = normalized(multiTemplate),
      Seq(),
    )

    val rendered = processedTemplate.renderRecords(records)

    val processed = processedTemplate.process(rendered.value)
    processed.value.endpoint should be("http://myExampleGroup.uk.example.com/10/Abcd1234/myTopic")

    normalized(processed.value.content) should be(
      normalized(
        """<xml><messages><message><topic>myTopic</topic><employee>Abcd1234</employee><order>10</order><groupDomain>myExampleGroup.uk</groupDomain>
          |    </message>
          |    <message>
          |       <topic>myTopic</topic>
          |       <employee>Efgh5678</employee>
          |       <order>11</order>
          |       <groupDomain>myExampleGroup.uk</groupDomain>
          |    </message>
          |  </messages>
          |</xml>""".stripMargin,
      ),
    )
  }

  private def normalized(s: String): String =
    s
      .replaceAll(">\\s+<", "><")
      .replaceAll("(?s)\\s+", " ").trim

}
