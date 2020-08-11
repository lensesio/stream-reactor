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
import java.util.concurrent.atomic.AtomicLong

import io.lenses.streamreactor.connect.aws.s3.config.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, ByteArraySourceData, BytesOutputRow}

import scala.util.Try

class BytesFormatWithSizesStreamReader(inputStreamFn: () => InputStream, fileSizeFn: () => Long, bucketAndPath: BucketAndPath, bytesWriteMode: BytesWriteMode) extends S3FormatStreamReader[ByteArraySourceData] {

  private val inputStream = inputStreamFn()

  private var recordNumber: Long = -1

  private val fileSizeCounter = new AtomicLong(fileSizeFn())

  override def hasNext: Boolean = fileSizeCounter.get() > 0

  override def next(): ByteArraySourceData = {
    recordNumber += 1
    val ret = ByteArraySourceData(BytesOutputRow(inputStream, bytesWriteMode), recordNumber)
    fileSizeCounter.addAndGet(- ret.data.bytesRead.get)
    ret
  }

  override def getLineNumber: Long = recordNumber

  override def close(): Unit = {
    Try {
      inputStream.close()
    }
  }

  override def getBucketAndPath: BucketAndPath = bucketAndPath

}
