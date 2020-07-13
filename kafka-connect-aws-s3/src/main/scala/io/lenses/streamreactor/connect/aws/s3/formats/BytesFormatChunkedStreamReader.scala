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

import io.lenses.streamreactor.connect.aws.s3.config.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, ByteArraySourceData, BytesOutputRow}

import scala.util.Try

class BytesFormatChunkedStreamReader(inputStreamFn: () => InputStream, fileSizeFn: () => Long, bucketAndPath: BucketAndPath, bytesWriteMode: BytesWriteMode, chunkSizeBytes: Int) extends S3FormatStreamReader[ByteArraySourceData] {

  private val inputStream = inputStreamFn()
  private var recordNumber: Long = -1

  private val fileSizeCounter = new FileSizeCounter(fileSizeFn())

  override def hasNext: Boolean = fileSizeCounter.getRemaining > 0

  def readNextChunk(inputStream: InputStream): Array[Byte] = {
    val numBytes = Set(fileSizeCounter.getRemaining, chunkSizeBytes.longValue()).min.intValue()
    val bArray = Array.ofDim[Byte](numBytes)
    inputStream.read(bArray, 0, numBytes)
    fileSizeCounter.decrementBy(numBytes)
    bArray
  }

  override def next(): ByteArraySourceData = {
    recordNumber += 1
    val nextChunk = readNextChunk(inputStream)
    ByteArraySourceData(BytesOutputRow(nextChunk, bytesWriteMode), recordNumber)
  }

  override def getLineNumber: Long = recordNumber

  override def close(): Unit = {
    Try {
      inputStream.close()
    }
  }

  override def getBucketAndPath: BucketAndPath = bucketAndPath

}
