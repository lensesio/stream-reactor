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

import java.io.{InputStream, InputStreamReader}

import au.com.bytecode.opencsv.CSVReader
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, CsvSourceData}

import scala.util.Try

class CsvFormatStreamReader(inputStreamFn: () => InputStream, bucketAndPath: BucketAndPath, readHeaders: Boolean) extends S3FormatStreamReader[CsvSourceData] {

  private val inputStreamReader = new InputStreamReader(inputStreamFn())
  private val csvReader = new CSVReader(inputStreamReader)
  private val iterator = new CsvReaderIteratorAdaptor(csvReader)

  private var columnHeaders: Option[Array[String]] = None
  private var lineNumber: Long = -1

  private def inventColumnHeaders(size: Int): Option[Array[String]] = Some((1 to size).map(n => s"col$n").toArray)

  override def hasNext: Boolean = iterator.hasNext

  def discoverColumnHeaders(): Option[Array[String]] = {

    if (readHeaders && iterator.hasNext) {
      lineNumber += 1
      Some(iterator.next())
    } else {
      iterator.peek match {
        case Some(value) => inventColumnHeaders(value.length)
        case None => if (readHeaders) {
          throw new IllegalStateException("No column headers are available")
        } else {
          throw new IllegalStateException("No rows available from which to create column headers")
        }
      }

    }

  }

  override def next(): CsvSourceData = {
    if (columnHeaders.isEmpty) {
      columnHeaders = discoverColumnHeaders()
    }
    val colHeaders = columnHeaders.getOrElse(throw new IllegalArgumentException("No header rows found"))
    lineNumber += 1
    CsvSourceData(colHeaders.toList, iterator.next().toList, lineNumber)
  }

  override def getLineNumber: Long = lineNumber

  override def close(): Unit = {
    Try {
      csvReader.close()
    }
    Try {
      inputStreamReader.close()
    }

  }

  override def getBucketAndPath: BucketAndPath = bucketAndPath

}
