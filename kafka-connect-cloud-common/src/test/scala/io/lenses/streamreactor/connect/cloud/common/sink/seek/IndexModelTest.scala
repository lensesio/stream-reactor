/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.seek
import cats.data.NonEmptyList
import cats.implicits.catsSyntaxOptionId
import io.circe.parser.decode
import io.circe.syntax._
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexFile._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class IndexModelTest extends AnyFunSuiteLike with Matchers with EitherValues {

  test("should write index file as json") {

    val lockOwner = UUID.randomUUID().toString
    val indexFile = IndexFile(
      lockOwner,
      Offset(1).some,
      PendingState(Offset(2), NonEmptyList.of(CopyOperation("bucket", "src", "dest", "myEtag"))).some,
    )
    val json = indexFile.asJson.spaces2

    val expectedJson =
      s"""
         |{
         |  "owner" : "$lockOwner",
         |  "committedOffset" : 1,
         |  "pendingState" : {
         |    "pendingOffset" : 2,
         |    "pendingOperations" : [
         |      {
         |        "type" : "copy",
         |        "bucket" : "bucket",
         |        "source" : "src",
         |        "destination" : "dest",
         |        "eTag" : "myEtag"
         |      }
         |    ]
         |  }
         |}
         |""".stripMargin.trim

    json shouldEqual expectedJson

    val decodedIndexFile = decode[IndexFile](json).value
    decodedIndexFile shouldEqual indexFile
  }

  // Ownership epoch: optional, written only when set, so locks
  // without an epoch stay byte-identical to the pre-epoch format, and older readers still decode.

  test("an index file without an epoch encodes exactly as the pre-epoch format (no epoch key)") {
    val indexFile = IndexFile("owner-1", Offset(7).some, None)

    indexFile.asJson.noSpaces shouldBe """{"owner":"owner-1","committedOffset":7,"pendingState":null}"""
  }

  test("an index file with an epoch encodes it and round-trips") {
    val indexFile = IndexFile("owner-1", Offset(7).some, None, epoch = Some(1731700000123L))
    val json      = indexFile.asJson.noSpaces

    json shouldBe """{"owner":"owner-1","committedOffset":7,"pendingState":null,"epoch":1731700000123}"""
    decode[IndexFile](json).value shouldBe indexFile
  }

  test("a pre-epoch lock decodes with epoch = None") {
    decode[IndexFile]("""{"owner":"old","committedOffset":3,"pendingState":null}""").value shouldBe
      IndexFile("old", Offset(3).some, None, epoch = None)
  }

  test("an explicit null epoch decodes as None") {
    decode[IndexFile]("""{"owner":"old","committedOffset":3,"pendingState":null,"epoch":null}""").value.epoch shouldBe
      None
  }

  test("format compatibility: an older connector version's decoder still reads a lock that carries an epoch") {
    // Not production code under test: this pins the rolling-upgrade assumption the epoch relies on.
    // An older version decodes locks with the same derived decoder, minus the epoch field, and
    // must ignore the new key rather than fail on it.
    final case class PreEpochIndexFile(
      owner:           String,
      committedOffset: Option[Offset],
      pendingState:    Option[PendingState],
    )
    val preEpochDecoder: io.circe.Decoder[PreEpochIndexFile] = io.circe.generic.semiauto.deriveDecoder

    val json = IndexFile("new", Offset(9).some, None, epoch = Some(5L)).asJson.noSpaces

    decode[PreEpochIndexFile](json)(preEpochDecoder).value shouldBe PreEpochIndexFile("new", Offset(9).some, None)
  }

}
