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
import java.util.Scanner

import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, StringSourceData}

import scala.util.Try

class TextFormatStreamReader(inputStreamFn: () => InputStream, bucketAndPath: BucketAndPath) extends S3FormatStreamReader[StringSourceData] {

  private val inputStream: InputStream = inputStreamFn()
  private val scanner = new Scanner(new InputStreamReader(inputStream))
  private var lineNumber: Long = -1

  override def close(): Unit = {
    Try(scanner.close())
    Try(inputStream.close())
  }

  override def hasNext: Boolean = {
    scanner.hasNextLine
  }

  override def next(): StringSourceData = {
    lineNumber += 1
    StringSourceData(scanner.nextLine(), lineNumber)
  }

  override def getBucketAndPath: BucketAndPath = bucketAndPath

  override def getLineNumber: Long = lineNumber
}
