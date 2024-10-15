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

class ParquetSeekableInputStream(recreateStreamF: () => Either[Throwable, InputStream])
    extends SeekableInputStream
    with LazyLogging {

  private var pos: Long = 0

  private var inputStream: InputStream = recreateStreamF().leftMap(throw _).merge

  override def getPos: Long = {
    logger.debug("Retrieving position: " + pos)
    pos
  }

  override def seek(newPos: Long): Unit = {
    if (newPos < pos) {
      logger.debug(s"Seeking from $pos to position $newPos - (Going backwards!)")

      inputStream.close()
      logger.debug("before input stream: {}", inputStream)
      inputStream = recreateStreamF().leftMap(throw _).merge
      logger.debug("after input stream: {}", inputStream)

      val skipped = inputStream.skip(newPos)
      logger.debug(s"Attempted to skip to $newPos, actually skipped $skipped bytes")

      if (skipped != newPos) {
        throw new EOFException(s"Failed to seek to position $newPos, only skipped $skipped bytes")
      }
    } else {
      logger.debug(s"Seeking from $pos to position $newPos")
      val skipped = inputStream.skip(newPos - pos)
      logger.debug(s"Attempted to skip to ${newPos - pos}, actually skipped $skipped bytes")

      if (skipped != (newPos - pos)) {
        throw new EOFException(s"Failed to seek to position $newPos, only skipped $skipped bytes")
      }
    }
    pos = newPos

  }

  override def read(buf: ByteBuffer): Int = {
    val bytesToRead = buf.remaining()
    val tempArray   = new Array[Byte](bytesToRead)
    val bytesRead   = inputStream.read(tempArray, 0, bytesToRead)

    if (bytesRead == -1) {
      return -1
    }

    buf.put(tempArray, 0, bytesRead)
    pos += bytesRead
    bytesRead
  }

  override def readFully(bytes: Array[Byte]): Unit = {
    var bytesRead = 0
    while (bytesRead < bytes.length) {
      val result = inputStream.read(bytes, bytesRead, bytes.length - bytesRead)
      logger.debug(s"Read $result bytes, total bytes read: ${bytesRead + result}")

      if (result == -1) {
        throw new EOFException(s"Reached the end of stream with ${bytes.length - bytesRead} bytes left to read")
      }
      bytesRead += result
      pos += result
    }
  }

  override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit = {
    var bytesRead = 0
    while (bytesRead < len) {
      val result = inputStream.read(bytes, start + bytesRead, len - bytesRead)
      logger.debug(s"Read $result bytes, total bytes read: ${bytesRead + result}")

      if (result == -1) {
        throw new EOFException(s"Reached the end of stream with ${len - bytesRead} bytes left to read")
      }
      bytesRead += result
      pos += result
    }
  }

  override def readFully(buf: ByteBuffer): Unit = {
    val bytesToRead = buf.remaining()
    val tempArray   = new Array[Byte](bytesToRead)
    var bytesRead   = 0

    while (bytesRead < bytesToRead) {
      val result = inputStream.read(tempArray, bytesRead, bytesToRead - bytesRead)
      logger.debug(s"Read $result bytes, total bytes read: ${bytesRead + result}")

      if (result == -1) {
        throw new EOFException(s"Reached the end of stream with ${bytesToRead - bytesRead} bytes left to read")
      }
      bytesRead += result
      pos += result
    }

    buf.put(tempArray, 0, bytesRead)
    ()
  }

  override def read(): Int = {
    val result = inputStream.read()
    if (result != -1) {
      pos += 1
    }
    result
  }

  override def skip(n: Long): Long = {
    super.skip(n)
    pos = pos + n
    n
  }

}
