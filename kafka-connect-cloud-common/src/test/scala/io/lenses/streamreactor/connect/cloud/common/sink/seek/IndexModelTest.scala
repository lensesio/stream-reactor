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

}
