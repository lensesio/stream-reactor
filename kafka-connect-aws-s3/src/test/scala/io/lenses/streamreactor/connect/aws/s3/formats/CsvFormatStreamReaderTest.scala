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
package io.lenses.streamreactor.connect.aws.s3.formats

import io.lenses.streamreactor.connect.aws.s3.config.ObjectMetadata
import io.lenses.streamreactor.connect.aws.s3.config.StreamReaderInput
import io.lenses.streamreactor.connect.aws.s3.formats.reader.CsvStreamReader
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.ContextConstants.LineKey
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData
import org.apache.kafka.connect.source.SourceRecord
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava

class CsvFormatStreamReaderTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private val bucketAndPath = mock[S3Location]

  "next" should "throw an error when you try and read an empty file when headers are configured" in {
    val reader = setUpReader(List(), includesHeaders = true)
    intercept[FormatWriterException] {
      reader.next()
    }.getMessage should be("No column headers are available")
    reader.close()
  }

  "next" should "throw an error when you try to read an empty file" in {
    val reader = setUpReader(List(), includesHeaders = false)
    intercept[FormatWriterException] {
      reader.next()
    }.getMessage should be(
      "Invalid state reached: the file content has been consumed, no further calls to next() are possible.",
    )
    reader.close()
  }

  "next" should "throw error when you call next incorrectly when headers are configured" in {
    val reader = setUpReader(List(SampleData.csvHeader), includesHeaders = true)
    intercept[FormatWriterException] {
      reader.next()
    }.getMessage should be(
      "Invalid state reached: the file content has been consumed, no further calls to next() are possible.",
    )
    reader.close()
  }

  "next" should "read multiple rows from a CSV file with headers" in {

    val reader  = setUpReader(SampleData.recordsAsCsvWithHeaders, includesHeaders = true)
    val results = reader.toList

    results.size should be(3)
    reader.getLineNumber should be(3)
    results.zipWithIndex.foreach {
      case (result, index) => result.sourceOffset().get(LineKey) shouldBe (index + 1).toString
    }
    checkResult(results)
    reader.close()
  }

  "next" should "read multiple rows from a CSV file without headers" in {

    val reader  = setUpReader(SampleData.recordsAsCsv, includesHeaders = false)
    val results = reader.toList

    results.size should be(3)
    reader.getLineNumber should be(2)
    results.zipWithIndex.foreach { case (result, index) => result.sourceOffset().get(LineKey) should be(index) }
    checkResult(results)

  }

  "getBucketAndPath" should "retain bucket and path from setup" in {
    val reader = setUpReader(SampleData.recordsAsCsv, includesHeaders = false)
    reader.getBucketAndPath should be(bucketAndPath)
  }

  private def setUpReader(recordsToReturn: List[String], includesHeaders: Boolean) =
    new CsvStreamReader(
      StreamReaderInput(
        new ByteArrayInputStream(
          recordsToReturn.mkString(System.lineSeparator()).getBytes(),
        ),
        bucketAndPath,
        ObjectMetadata(
          bucketAndPath.bucket,
          "myBucket:myPath",
          0,
          Instant.now(),
        ),
        hasEnvelope = false,
        () => Right(new ByteArrayInputStream(recordsToReturn.mkString(System.lineSeparator()).getBytes())),
        0,
        Topic("topic"),
        Map.empty.asJava,
      ),
      hasHeaders = includesHeaders,
    )

  private def checkResult(results: Seq[SourceRecord]) = {
    results.head.value() should be(""""sam","mr","100.43"""")
    results(1).value() should be(""""laura","ms","429.06"""")
    results(2).value() should be(""""tom",,"395.44"""")
  }
}
