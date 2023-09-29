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

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData
import io.lenses.streamreactor.connect.cloud.formats.writer.ByteArraySinkData
import io.lenses.streamreactor.connect.cloud.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.formats.writer.StringSinkData
import io.lenses.streamreactor.connect.cloud.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.model.Offset
import io.lenses.streamreactor.connect.cloud.model.Topic
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class TopicsTransformersTest extends AnyFunSuite with Matchers {
  test("unmatched topic returns the same message detail") {
    val messageDetail = MessageDetail(
      StringSinkData("key"),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val topicsTransformers = TopicsTransformers(Map.empty)
    topicsTransformers.transform(messageDetail).getOrElse(
      fail("Should have returned a message"),
    ) eq messageDetail shouldBe true
  }
  test("apply the transformation for a given topic") {
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
    val topicsTransformers = TopicsTransformers(
      Map(
        Topic("topic1") -> ((message: MessageDetail) => {
          message.copy(key = actualKey).asRight
        }),
        Topic("topic2") -> ((message: MessageDetail) => {
          message.copy(key = StringSinkData("another different key")).asRight
        }),
      ),
    )
    topicsTransformers.transform(messageDetail).getOrElse(
      fail("Should have returned a message"),
    ) shouldBe messageDetail.copy(key = actualKey)
  }
  test("return the error from the transformer") {
    val messageDetail = MessageDetail(
      StringSinkData("key"),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val topicsTransformers = TopicsTransformers(
      Map(
        Topic("topic1") -> ((_: MessageDetail) => {
          Left(new RuntimeException("Error"))
        }),
        Topic("topic2") -> ((message: MessageDetail) => {
          message.copy(key = StringSinkData("another different key")).asRight
        }),
      ),
    )
    topicsTransformers.transform(messageDetail) match {
      case Left(value: RuntimeException) => value.getMessage shouldBe "Error"
      case Left(_)  => fail("Should have failed with RuntimeException.")
      case Right(_) => fail("Should have returned an error.")
    }
  }
}
