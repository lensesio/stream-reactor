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
package io.lenses.streamreactor.connect.cloud.common.source

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.connect.source.SourceTaskContext
import org.mockito.Answers
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava

class SourceContextReaderTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private val sourceTaskContext = mock[SourceTaskContext](Answers.RETURNS_DEEP_STUBS)

  private val bucketName = "bucket"
  private val prefixName = "prefixName"

  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator
  private val rootLocation = CloudLocation(bucketName, prefixName.some)

  private val filePath = "prefixName/file.json"

  private val mapKey = Map(
    "container" -> bucketName,
    "prefix"    -> prefixName,
  ).asJava

  val contextReaderFn = SourceContextReader.getCurrentOffset(() => sourceTaskContext) _

  val nowMillis = Instant.now().toEpochMilli

  "getCurrentOffset" should "return offset when one has been defined" in {
    val mapValue = Map[String, AnyRef](
      "path" -> filePath,
      "line" -> "100",
      "ts"   -> nowMillis.toString,
    ).asJava

    when(sourceTaskContext.offsetStorageReader().offset(mapKey)).thenReturn(mapValue)

    contextReaderFn(rootLocation) should be(
      Some(
        rootLocation
          .withPath(filePath)
          .atLine(100)
          .withTimestamp(Instant.ofEpochMilli(nowMillis)),
      ),
    )
  }

  "getCurrentOffset" should "return none when no offset has been defined" in {

    when(sourceTaskContext.offsetStorageReader().offset(mapKey)).thenReturn(null)

    contextReaderFn(rootLocation) should be(None)

  }

  "getCurrentOffset" should "return none when invalid offset has been defined" in {

    val mapValue = Map[String, AnyRef](
      "path" -> filePath,
      "line" -> "???",
      "ts"   -> nowMillis.toString,
    ).asJava

    when(sourceTaskContext.offsetStorageReader().offset(mapKey)).thenReturn(mapValue)

    contextReaderFn(rootLocation) should be(None)

  }
}
