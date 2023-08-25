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

import com.google.common.io.ByteStreams
import io.lenses.streamreactor.connect.aws.s3.config.StreamReaderInput
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.SourceWatermark
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord

import scala.util.Try

class BytesStreamFileReader(
  input:          StreamReaderInput,
  bytesWriteMode: BytesWriteMode,
) extends S3StreamReader {

  private var consumed: Boolean = false

  override def hasNext: Boolean = !consumed && input.metadata.size > 0L

  override def next(): SourceRecord = {
    val fileAsBytes = ByteStreams.toByteArray(input.stream)
    val row         = bytesWriteMode.read(fileAsBytes)
    consumed = true

    new SourceRecord(
      input.sourcePartition,
      SourceWatermark.offset(input.bucketAndPath, getLineNumber, input.metadata.lastModified),
      input.targetTopic.value,
      input.targetPartition,
      Schema.BYTES_SCHEMA,
      row.key,
      Schema.BYTES_SCHEMA,
      row.value,
    )
  }

  override def getLineNumber: Long = if (consumed) 0 else -1

  override def close(): Unit = {
    val _ = Try {
      input.stream.close()
    }
  }

  override def getBucketAndPath: S3Location = input.bucketAndPath

}
