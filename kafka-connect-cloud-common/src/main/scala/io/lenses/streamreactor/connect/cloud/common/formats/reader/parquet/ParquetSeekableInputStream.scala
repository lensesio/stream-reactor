/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats.reader.parquet

import cats.implicits.toBifunctorOps
import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.io.SeekableInputStream

import java.io.EOFException
import java.io.InputStream
import java.nio.ByteBuffer

/**
  * A custom implementation of `SeekableInputStream` for reading Parquet files.
  * This class supports seeking to a specific position in the input stream and reading data.
  *
  * @param recreateStreamF a function that recreates the input stream.
  * @throws RuntimeException if the input stream cannot be created.
  */
class ParquetSeekableInputStream(recreateStreamF: () => Either[Throwable, InputStream])
    extends SeekableInputStream
    with LazyLogging {

  private var pos: Long = 0

  private var inputStream: InputStream = recreateStreamF().leftMap(throw _).merge
  private val maxChunkSize = 10 * 1024 * 1024 // 10 MB

  /**
    * Returns the current position in the input stream.
    *
    * @return the current position.
    */
  override def getPos: Long = {
    logger.debug("Retrieving position: " + pos)
    pos
  }

  /**
    * Seeks to the specified position in the input stream.
    *
    * @param requestedPos the position to seek to.
    * @throws EOFException if the end of the stream is reached before the requested position.
    */
  override def seek(requestedPos: Long): Unit = {
    def reloadInputStream(): Unit = {
      inputStream.close()
      val oldIsRef = inputStream
      inputStream = recreateStreamF().leftMap(throw _).merge
      logger.trace("reloading input stream, old input stream ref: {}, new input stream: {}", oldIsRef, inputStream)
    }
    val bytesToSkip = if (requestedPos < pos) {
      logger.debug(s"Seeking from $pos to position $requestedPos - (Going backwards!)")
      reloadInputStream()
      requestedPos
    } else {
      logger.debug(s"Seeking from $pos to position $requestedPos")
      requestedPos - pos
    }
    val skipped = inputStream.skip(bytesToSkip)
    validateSeek(requestedPos, bytesToSkip, skipped)
    pos = requestedPos
  }

  /**
    * Reads data from the input stream in chunks and handles the end-of-stream condition.
    *
    * @param buffer the array to read data into.
    * @param offset the start offset in the array.
    * @param length the number of bytes to read.
    * @return the total number of bytes read, or -1 if the end of the stream is reached before any data is read.
    */
  private def readInChunks(buffer: Array[Byte], offset: Int, length: Int): Int = {
    var totalBytesRead = 0
    while (totalBytesRead < length) {
      val chunkSize = Math.min(maxChunkSize, length - totalBytesRead)
      val bytesRead = inputStream.read(buffer, offset + totalBytesRead, chunkSize)
      if (bytesRead == -1) {
        return if (totalBytesRead == 0) -1 else totalBytesRead
      }
      totalBytesRead += bytesRead
      pos += bytesRead
    }
    totalBytesRead
  }

  /**
    * Reads data from the input stream into the specified `ByteBuffer`.
    *
    * @param buf the buffer to read data into.
    * @return the number of bytes read, or -1 if the end of the stream is reached.
    */
  override def read(buf: ByteBuffer): Int = {
    val bytesToRead = buf.remaining()
    val tempArray   = new Array[Byte](bytesToRead)
    val bytesRead   = readInChunks(tempArray, 0, bytesToRead)
    if (bytesRead > 0) buf.put(tempArray, 0, bytesRead)
    bytesRead
  }

  /**
    * Reads data from the input stream into the specified byte array.
    *
    * @param bytes the array to read data into.
    * @throws EOFException if the end of the stream is reached before the array is filled.
    */
  override def readFully(bytes: Array[Byte]): Unit = {
    val bytesRead = readInChunks(bytes, 0, bytes.length)
    validateBytesRead(bytes.length, bytesRead)
  }

  /**
    * Reads data from the input stream into the specified byte array starting at the given offset.
    *
    * @param bytes the array to read data into.
    * @param start the start offset in the array.
    * @param len the number of bytes to read.
    * @throws EOFException if the end of the stream is reached before the specified number of bytes is read.
    */
  override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit = {
    val bytesRead = readInChunks(bytes, start, len)
    validateBytesRead(len, bytesRead)
  }

  /**
    * Reads data from the input stream into the specified `ByteBuffer`.
    *
    * @param buf the buffer to read data into.
    * @throws EOFException if the end of the stream is reached before the buffer is filled.
    */
  override def readFully(buf: ByteBuffer): Unit = {
    val bytesToRead = buf.remaining()
    val tempArray   = new Array[Byte](bytesToRead)
    val bytesRead   = readInChunks(tempArray, 0, bytesToRead)
    validateBytesRead(bytesToRead, bytesRead)
    buf.put(tempArray, 0, bytesRead)
    ()
  }

  /**
    * Reads data from the input stream into the specified byte array.
    * This method uses chunking to handle large reads efficiently.
    *
    * @param bytes the array to read data into.
    * @return the total number of bytes read, or -1 if the end of the stream is reached before any data is read.
    */
  override def read(bytes: Array[Byte]): Int = readInChunks(bytes, 0, bytes.length)

  /**
    * Reads the next byte of data from the input stream.
    *
    * @return the next byte of data, or -1 if the end of the stream is reached.
    */
  override def read(): Int = {
    val result = inputStream.read()
    if (result != -1) {
      pos += 1
    }
    result
  }

  /**
    * Skips over and discards the specified number of bytes from the input stream.
    *
    * @param n the number of bytes to skip.
    * @return the actual number of bytes skipped.
    */
  override def skip(n: Long): Long = {
    val skipped = inputStream.skip(n)
    pos += skipped
    skipped
  }

  /**
    * Validates that the seek operation was successful.
    *
    * @param requestedPos the requested position.
    * @param bytesToSkip the number of bytes to skip.
    * @param skipped the actual number of bytes skipped.
    * @throws EOFException if the seek operation failed.
    */
  private def validateSeek(requestedPos: Long, bytesToSkip: Long, skipped: Long): Unit = {
    logger.debug(s"Attempted to skip to $bytesToSkip, actually skipped $skipped bytes")

    if (skipped != bytesToSkip) {
      throw new EOFException(s"Failed to seek to position $requestedPos, only skipped $skipped bytes")
    }
  }

  /**
    * Validates that the number of bytes read matches the expected number of bytes to read.
    * Throws an `EOFException` if the end of the stream is reached before the expected number of bytes is read.
    *
    * @param bytesToRead the expected number of bytes to read.
    * @param bytesRead the actual number of bytes read.
    * @throws EOFException if the actual number of bytes read is less than the expected number of bytes to read.
    */
  private def validateBytesRead(bytesToRead: Int, bytesRead: Int): Unit =
    if (bytesRead < bytesToRead) {
      throw new EOFException(s"Reached the end of stream with ${bytesToRead - bytesRead} bytes left to read")
    }

}
