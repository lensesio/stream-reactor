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

import java.io.InputStream

import com.google.common.io.ByteStreams
import io.lenses.streamreactor.connect.aws.s3.config.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, ByteArraySourceData, BytesOutputRow}

import scala.util.Try

class BytesFormatStreamFileReader(inputStreamFn: () => InputStream, fileSizeFn: () => Long, bucketAndPath: BucketAndPath, bytesWriteMode: BytesWriteMode) extends S3FormatStreamReader[ByteArraySourceData] {

  private var consumed : Boolean = false
  private val inputStream = inputStreamFn()
  private val fileSize = fileSizeFn()

  override def hasNext: Boolean = !consumed && fileSize > 0L

  override def next(): ByteArraySourceData = {
    val nextChunk = ByteStreams.toByteArray(inputStream)
    consumed = true
    ByteArraySourceData(BytesOutputRow(nextChunk, bytesWriteMode), getLineNumber)
  }

  override def getLineNumber: Long = if (consumed) 0 else -1

  override def close(): Unit = {
    Try {
      inputStream.close()
    }
  }

  override def getBucketAndPath: BucketAndPath = bucketAndPath

}
