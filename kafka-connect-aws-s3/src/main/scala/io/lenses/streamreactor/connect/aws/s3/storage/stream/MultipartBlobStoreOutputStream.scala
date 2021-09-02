
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
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.processing._
import io.lenses.streamreactor.connect.aws.s3.storage.MultiPartUploadState

import java.io.OutputStream
import java.nio.ByteBuffer

object MultipartBlobStoreOutputStream {
  def apply(initialName: RemoteS3PathLocation,
            initialOffset: Offset,
            updateOffsetFn: Offset => () => Unit,
            minAllowedMultipartSize: Int)(implicit queueProcessor: BlockingQueueProcessor): Either[Throwable, MultipartBlobStoreOutputStream] = {

    MultipartBlobStoreOutputStream(initialName, initialOffset, updateOffsetFn, minAllowedMultipartSize)

  }
}

class MultipartBlobStoreOutputStream(
                                      initialName: RemoteS3PathLocation,
                                      initialOffset: Offset,
                                      updateOffsetFn: Offset => () => Unit,
                                      minAllowedMultipartSize: Int = 5242880

                                    )(implicit queueProcessor: BlockingQueueProcessor) extends OutputStream with LazyLogging with S3OutputStream {



  private val buffer: ByteBuffer = ByteBuffer.allocate(minAllowedMultipartSize)
  private var pointer = 0
  private var uploadState: Option[MultiPartUploadState] = None
  private var completed = false

  queueProcessor.enqueue(InitUploadProcessorOperation(initialOffset, initialName, setStateCallbackFn))

  private def getLatestStateFn: () => MultiPartUploadState = () => {
    logger.trace(s"Retrieving state $uploadState")
    uploadState.getOrElse(throw new IllegalStateException("No state has been set"))
  }

  private def setStateCallbackFn(state: MultiPartUploadState): Unit = {
    uploadState.fold {} {
      us =>
        if (state.upload.blobName() != us.upload.blobName()) {
          throw new IllegalStateException("Upload blobname has changed, it should be fixed")
        }
        logger.trace(s"Replacing state, old state " +
          s"(${us.parts.size} ${us.upload.blobName()}) new state: " +
          s"(${state.parts.size}) ${state.upload.blobName()}")
    }
    uploadState = Some(state)
  }

  override def write(bytes: Array[Byte], startOffset: Int, numberOfBytes: Int): Unit = {

    require(bytes != null && bytes.nonEmpty, "Bytes must be provided")
    require(
      validateRange(startOffset, bytes.length) &&
        numberOfBytes > 0 &&
        validateRange(startOffset + numberOfBytes, bytes.length)
    )

    val remainingOnBuffer = buffer.remaining()

    val numberOfBytesToAppend = if (numberOfBytes < remainingOnBuffer) numberOfBytes else remainingOnBuffer

    appendToBuffer(bytes, startOffset, numberOfBytesToAppend)

    if (remainingOnBuffer == numberOfBytesToAppend) {
      uploadPart(minAllowedMultipartSize, None)
      val remaining = numberOfBytes - remainingOnBuffer
      if (remaining > 0) {
        write(
          bytes,
          startOffset + remainingOnBuffer,
          numberOfBytes - remainingOnBuffer
        )
      }
    }
  }

  override def write(b: Int): Unit = {
    buffer.put(b.toByte)
    if (!buffer.hasRemaining) {
      uploadPart(minAllowedMultipartSize, None)
    }
    pointer += 1
  }

  private def uploadPart(size: Long, kafkaOffset: Option[Offset]): Unit = {
    val toUpload = getCurrentBufferContents.slice(0, size.toInt)
    buffer.clear
    queueProcessor.enqueue(UploadPartProcessorOperation(kafkaOffset, getLatestStateFn, toUpload, setStateCallbackFn))
  }

  private def appendToBuffer(bytes: Array[Byte], startOffset: Int, numberOfBytes: Int): Unit = {
    buffer.put(bytes, startOffset, numberOfBytes)
    pointer += numberOfBytes
  }

  override def complete(finalDestination: RemoteS3PathLocation, kafkaOffset: Offset): Unit = {

    if (completed) return

    if (buffer.position() > 0) {
      val nextPart = buffer.array().slice(0, buffer.position)
      queueProcessor.enqueue(UploadPartProcessorOperation(Some(kafkaOffset), getLatestStateFn, nextPart, setStateCallbackFn))
    }

    buffer.clear()
    queueProcessor.enqueue(
      CompleteUploadProcessorOperation(kafkaOffset, getLatestStateFn),
      RenameFileProcessorOperation(kafkaOffset, initialName, finalDestination, updateOffsetFn(kafkaOffset))
    )

    completed = true
  }

  private def validateRange(startOffset: Int, numberOfBytes: Int) = startOffset >= 0 && startOffset <= numberOfBytes

  def getCurrentBufferContents: Array[Byte] = {
    buffer.array().slice(0, minAllowedMultipartSize - buffer.remaining())
  }

  override def getPointer: Long = pointer
}
