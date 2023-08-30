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

import io.lenses.streamreactor.connect.aws.s3.formats.reader.CsvStreamReader
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream

class CsvFormatStreamReaderTest extends AnyFlatSpec with Matchers with MockitoSugar {

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
    checkResult(results)
    reader.close()
  }

  "next" should "read multiple rows from a CSV file without headers" in {

    val reader  = setUpReader(SampleData.recordsAsCsv, includesHeaders = false)
    val results = reader.toList

    results.size should be(3)
    checkResult(results)

  }

  private def setUpReader(recordsToReturn: List[String], includesHeaders: Boolean): CsvStreamReader =
    new CsvStreamReader(
      new ByteArrayInputStream(
        recordsToReturn.mkString(System.lineSeparator()).getBytes(),
      ),
      hasHeaders = includesHeaders,
    )

  private def checkResult(results: Seq[String]) = {
    results.head should be(""""sam","mr","100.43"""")
    results(1) should be(""""laura","ms","429.06"""")
    results(2) should be(""""tom",,"395.44"""")
  }
}
