package io.lenses.streamreactor.connect.aws.s3.storage

import java.io.OutputStream
import java.nio.ByteBuffer

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.BucketAndPath

class MultipartBlobStoreOutputStream(
                                      bucketAndPath: BucketAndPath,
                                      minAllowedMultipartSize: Int
                                    )(
                                      implicit storageInterface: StorageInterface
                                    ) extends OutputStream with LazyLogging with S3OutputStream {

  private var uploadState: MultiPartUploadState = storageInterface.initUpload(bucketAndPath)
  private val buffer: ByteBuffer = ByteBuffer.allocate(minAllowedMultipartSize)
  private var pointer = 0
  private var uploadedBytes: Long = 0

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
      uploadPart(minAllowedMultipartSize)
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
      uploadPart(minAllowedMultipartSize)
    }
    pointer += 1
  }

  private def uploadPart(size: Long): Unit = {
    uploadState = storageInterface.uploadPart(uploadState, getCurrentBufferContents, size)
    buffer.clear
    uploadedBytes += size
  }

  private def appendToBuffer(bytes: Array[Byte], startOffset: Int, numberOfBytes: Int): Unit = {
    buffer.put(bytes, startOffset, numberOfBytes)
    pointer += numberOfBytes
  }

  def complete(): Boolean = {

    if (buffer.position() > 0)
      uploadState = storageInterface.uploadPart(uploadState, buffer.array(), buffer.position)

    uploadState match {
      case state if state.parts.nonEmpty =>
        storageInterface.completeUpload(state)
        buffer.clear()
        true

      case _ => false
    }

  }
  private def validateRange(startOffset: Int, numberOfBytes: Int) = startOffset >= 0 && startOffset <= numberOfBytes

  def getCurrentBufferContents: Array[Byte] = {
    buffer.array().slice(0, minAllowedMultipartSize - buffer.remaining())
  }

  override def getPointer(): Long = pointer
}
