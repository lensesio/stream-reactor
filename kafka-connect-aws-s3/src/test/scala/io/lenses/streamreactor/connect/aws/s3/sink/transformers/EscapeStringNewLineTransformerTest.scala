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

import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class EscapeStringNewLineTransformerTest extends AnyFunSuite with Matchers {
  test("escapes new lines for a String") {

    val messageDetail = MessageDetail(
      NullSinkData(None),
      StringSinkData("test\nstring\nnew\nline"),
      Map.empty,
      None,
      Topic("topic1"),
      0,
      Offset(3),
    )
    EscapeStringNewLineTransformer.transform(messageDetail) match {
      case Left(value) => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) =>
        value.value shouldBe StringSinkData("test\\nstring\\nnew\\nline")
    }
  }
  test("escape string in a nested Map") {
    val messageDetail = MessageDetail(
      NullSinkData(None),
      MapSinkData(Map("key1" -> "value1", "key2" -> "value2\nvalue3").asJava, None),
      Map.empty,
      None,
      Topic("topic1"),
      0,
      Offset(3),
    )
    EscapeStringNewLineTransformer.transform(messageDetail) match {
      case Left(value) => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) =>
        value.value match {
          case MapSinkData(map, _) => map.get("key2") shouldBe "value2\\nvalue3"
          case _                   => fail("Should have returned a MapSinkData")
        }

    }
  }
  test("escapes a string in a map in an array") {
    val messageDetail = MessageDetail(
      NullSinkData(None),
      MapSinkData(
        Map[String, Any]("key1" -> "value1", "key2" -> List[String]("value2\nvalue3", "value4").asJava).asJava,
        None,
      ),
      Map.empty,
      None,
      Topic("topic1"),
      0,
      Offset(3),
    )
    EscapeStringNewLineTransformer.transform(messageDetail) match {
      case Left(value) => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) =>
        value.value match {
          case MapSinkData(map, _) =>
            map.get("key2").asInstanceOf[java.util.List[_]].asScala.map(_.toString) shouldBe List("value2\\nvalue3",
                                                                                                  "value4",
            )
          case _ => fail("Should have returned a MapSinkData")
        }

    }
  }
  test("escapes a string in an array in a Connect Struct") {

    val struct = new Struct(SampleData.UserWithAddressSchema)
      .put("name", "sam")
      .put("title", "mr")
      .put("salary", 100.43)
      .put(
        "address",
        new Struct(SampleData.AddressSchema)
          .put("street", "Blue Moon\nStreet")
          .put("city", "city1")
          .put("country", "country1"),
      )
      .put("phone_numbers", List("123456789\n 123", "987654321").asJava)

    val messageDetail = MessageDetail(
      NullSinkData(None),
      StructSinkData(struct),
      Map.empty,
      None,
      Topic("topic1"),
      0,
      Offset(3),
    )

    EscapeStringNewLineTransformer.transform(messageDetail) match {
      case Left(value) => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) =>
        value.value match {
          case StructSinkData(struct) =>
            struct.get("address").asInstanceOf[Struct].get("street") shouldBe "Blue Moon\\nStreet"
            struct.get("phone_numbers").asInstanceOf[java.util.List[_]].asScala.map(_.toString) shouldBe List(
              "123456789\\n 123",
              "987654321",
            )
          case _ => fail("Should have returned a StructSinkData")
        }

    }
  }
}
