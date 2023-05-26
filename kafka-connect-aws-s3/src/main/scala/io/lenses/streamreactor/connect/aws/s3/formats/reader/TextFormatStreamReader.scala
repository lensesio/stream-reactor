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
package io.lenses.streamreactor.connect.aws.s3.formats.reader

import io.lenses.streamreactor.connect.aws.s3.formats.FormatWriterException
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location

import java.io.InputStream
import scala.io.Source
import scala.util.Try

class TextFormatStreamReader(inputStreamFn: () => InputStream, bucketAndPath: S3Location)
    extends S3FormatStreamReader[StringSourceData] {

  private val inputStream: InputStream = inputStreamFn()
  private val source        = Source.fromInputStream(inputStream, "UTF-8")
  protected val sourceLines = source.getLines()
  protected var lineNumber: Long = -1

  override def close(): Unit = { val _ = Try(source.close()) }

  override def hasNext: Boolean = sourceLines.hasNext

  override def next(): StringSourceData = {
    lineNumber += 1
    if (!sourceLines.hasNext) {
      throw FormatWriterException(
        "Invalid state reached: the file content has been consumed, no further calls to next() are possible.",
      )
    }
    StringSourceData(sourceLines.next(), lineNumber)
  }

  override def getBucketAndPath: S3Location = bucketAndPath

  override def getLineNumber: Long = lineNumber
}
