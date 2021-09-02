
/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.storage.stream

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.location.{LocalPathLocation, RemoteS3PathLocation}
import io.lenses.streamreactor.connect.aws.s3.processing.{BlockingQueueProcessor, UploadFileProcessorOperation}

import java.io.OutputStream


class BuildLocalOutputStream(
                              initialName: LocalPathLocation,
                              updateOffsetFn: Offset => () => Unit,
                              cleanUp: Boolean = true,
                            )(
                              implicit queueProcessor: BlockingQueueProcessor
                            ) extends OutputStream with LazyLogging with S3OutputStream {

  private val outputStream = initialName.toBufferedFileOutputStream

  private var pointer = 0

  override def write(bytes: Array[Byte], startOffset: Int, numberOfBytes: Int): Unit = {

    require(bytes != null && bytes.nonEmpty, "Bytes must be provided")
    val endOffset = startOffset + numberOfBytes
    require(
      validateRange(startOffset, bytes.length) &&
        numberOfBytes > 0 &&
        validateRange(endOffset, bytes.length)
    )

    outputStream.write(bytes.slice(startOffset, endOffset))
    pointer += endOffset - startOffset
  }

  override def write(b: Int): Unit = {
    outputStream.write(b)
    pointer += 1
  }

  override def complete(finalDestination: RemoteS3PathLocation, offset: Offset): Unit = {
    outputStream.close()
    queueProcessor.enqueue(UploadFileProcessorOperation(offset, initialName, finalDestination, updateOffsetFn(offset)))
    queueProcessor.process()
    close()
  }

  override def close(): Unit = {
    super.close()
    if (cleanUp) {
      initialName.delete()
    }
  }

  private def validateRange(startOffset: Int, numberOfBytes: Int) = startOffset >= 0 && startOffset <= numberOfBytes

  override def getPointer: Long = pointer

}
