/*
 * Copyright 2020 Lenses.io
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

import java.io.ByteArrayInputStream

import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, CsvSourceData}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CsvFormatStreamReaderTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private val bucketAndPath = mock[BucketAndPath]

  "next" should "throw an error when you try and read an empty file when headers are configured" in {
    val reader = setUpReader(List(), includesHeaders = true)
    intercept[IllegalStateException] {
      reader.next()
    }.getMessage should be("No column headers are available")
    reader.close()
  }

  "next" should "throw an error when you try to read an empty file when no headers are configured" in {
    val reader = setUpReader(List(), includesHeaders = false)
    intercept[IllegalStateException] {
      reader.next()
    }.getMessage should be("No rows available from which to create column headers")
    reader.close()
  }

  "next" should "throw error when you call next incorrectly when headers are configured" in {
    val reader = setUpReader(List(TestSampleSchemaAndData.csvHeader), includesHeaders = true)
    intercept[IndexOutOfBoundsException] {
      reader.next()
    }.getMessage should be("Unable to call next on reaching the end")
    reader.close()
  }

  "next" should "read multiple rows from a CSV file with headers" in {

    val reader = setUpReader(TestSampleSchemaAndData.recordsAsCsvWithHeaders, includesHeaders = true)
    val results: Seq[CsvSourceData] = reader.toList

    results.size should be(3)
    reader.getLineNumber should be(3)
    results.foreach(_.columnHeaders should be(List("name", "title", "salary")))
    results.zipWithIndex.foreach { case (result, index) => result.lineNumber should be(index + 1) }
    checkResult(results)
    reader.close()
  }

  "next" should "read multiple rows from a CSV file without headers" in {

    val reader = setUpReader(TestSampleSchemaAndData.recordsAsCsv, includesHeaders = false)
    val results: Seq[CsvSourceData] = reader.toList

    results.size should be(3)
    reader.getLineNumber should be(2)
    results.foreach(_.columnHeaders should be(List("col1", "col2", "col3")))
    results.zipWithIndex.foreach { case (result, index) => result.lineNumber should be(index) }
    checkResult(results)

  }

  "getBucketAndPath" should "retain bucket and path from setup" in {
    val reader = setUpReader(TestSampleSchemaAndData.recordsAsCsv, includesHeaders = false)
    reader.getBucketAndPath should be(bucketAndPath)
  }

  private def setUpReader(recordsToReturn: List[String], includesHeaders: Boolean) = {
    new CsvFormatStreamReader(
      inputStreamFn = () => new ByteArrayInputStream(
        recordsToReturn.mkString(System.lineSeparator()).getBytes()
      ),
      bucketAndPath = bucketAndPath,
      readHeaders = includesHeaders
    )
  }

  private def checkResult(results: Seq[CsvSourceData]) = {
    results(0).data should be(Array("sam", "mr", "100.43"))
    results(1).data should be(Array("laura", "ms", "429.06"))
    results(2).data should be(Array("tom", "", "395.44"))
  }
}
