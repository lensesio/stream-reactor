
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

import java.io.ByteArrayInputStream

import org.apache.parquet.io.{DelegatingSeekableInputStream, InputFile, SeekableInputStream}


class SeekableByteArrayInputStream(val bArr: Array[Byte]) extends ByteArrayInputStream(bArr) {
  def setPos(pos: Int): Unit = this.pos = pos

  def getPos: Int = this.pos
}

class ParquetInputFile(inputStream: SeekableByteArrayInputStream) extends InputFile {

  override def getLength: Long = inputStream.available()

  override def newStream(): SeekableInputStream = new DelegatingSeekableInputStream(inputStream) {
    override def getPos: Long = this.getStream.asInstanceOf[SeekableByteArrayInputStream].getPos.longValue

    override def seek(newPos: Long): Unit = getStream.asInstanceOf[SeekableByteArrayInputStream].setPos(newPos.intValue)
  }


}
