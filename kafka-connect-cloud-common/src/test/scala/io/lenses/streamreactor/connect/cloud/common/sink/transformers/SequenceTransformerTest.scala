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

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.ByteArraySinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class SequenceTransformerTest extends AnyFunSuite with Matchers {
  test("empty sequence returns the same message") {
    val messageDetail = MessageDetail(
      StringSinkData("key"),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val transformer = SequenceTransformer()
    transformer.transform(messageDetail) match {
      case Left(value)  => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) => value eq messageDetail shouldBe true
    }
  }
  test("apply all the transformers in the sequence") {
    val messageDetail = MessageDetail(
      StringSinkData("key"),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val actualKey = StringSinkData("different key")
    val transformer = SequenceTransformer(
      (message: MessageDetail) => message.copy(key = actualKey).asRight,
      (message: MessageDetail) => message.copy(key = StringSinkData("another different key")).asRight,
    )
    transformer.transform(messageDetail) match {
      case Left(value)  => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) => value shouldBe messageDetail.copy(key = StringSinkData("another different key"))
    }
  }
  test("return the error from the first transformer") {
    val messageDetail = MessageDetail(
      StringSinkData("key"),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val transformer = SequenceTransformer(
      (message: MessageDetail) => message.copy(key = StringSinkData("another different key")).asRight,
      (_: MessageDetail) => Left(new RuntimeException("Error from the second transformer")),
      (message: MessageDetail) => message.copy(value = StringSinkData("another different value")).asRight,
      (_: MessageDetail) => Left(new RuntimeException("Error from the fourth transformer")),
    )
    transformer.transform(messageDetail) match {
      case Left(value)  => value.getMessage shouldBe "Error from the second transformer"
      case Right(value) => fail(s"Should have returned an error: $value.")
    }
  }
}
