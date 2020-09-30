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

package io.lenses.streamreactor.connect.aws.s3.formats.parquet

import java.io.InputStream
import java.nio.ByteBuffer

import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.io.{DelegatingSeekableInputStream, SeekableInputStream}

class ParquetSeekableInputStream(inputStreamFn: () => InputStream) extends SeekableInputStream with LazyLogging {

  /**
    * The InceptionDelegatingInputStream delegates to a DelegatingInputStream for the read operations (so as to avoid
    * duplication of all the read code, and it delegates to the outer class for the position and seeking operations.
    *
    * This is obviously a massive workaround for the design of the library we are using as we cannot supply a HTTP input
    * stream that is seekable and therefore we need to recreate the inputStream if we want to seek backwards.
    *
    * We will therefore need to also recreate the InceptionDelegatingInputStream in the event we want to seek backwards.
    *
    * @param inputStream the actual inputStream containing Parquet data from S3
    */
  class InceptionDelegatingInputStream(inputStream: InputStream) extends DelegatingSeekableInputStream(inputStream) {
    override def getPos: Long = ParquetSeekableInputStream.this.getPos

    override def seek(newPos: Long): Unit = ParquetSeekableInputStream.this.seek(newPos)
  }


  private var pos: Long = 0

  private var inputStream: InputStream = _
  private var inceptionInputStream: DelegatingSeekableInputStream = _

  createInputStream()

  private def createInputStream(): Unit = {
    logger.debug(s"Recreating input stream")
    inputStream = inputStreamFn()
    inceptionInputStream = new InceptionDelegatingInputStream(inputStream)
  }


  override def getPos: Long = {
    logger.debug("Retrieving position: " + pos)
    pos
  }

  override def seek(newPos: Long): Unit = {
    logger.debug(s"Seeking from $pos to position $newPos")
    if (newPos < pos) {
      createInputStream()
      inputStream.skip(newPos)
    } else {
      inputStream.skip(newPos - pos)
    }
    pos = newPos

  }

  override def readFully(bytes: Array[Byte]): Unit = inceptionInputStream.readFully(bytes)

  override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit = inceptionInputStream.readFully(bytes, start, len)

  override def read(buf: ByteBuffer): Int = inceptionInputStream.read(buf)

  override def readFully(buf: ByteBuffer): Unit = inceptionInputStream.readFully(buf)

  override def read(): Int = inceptionInputStream.read()
}
