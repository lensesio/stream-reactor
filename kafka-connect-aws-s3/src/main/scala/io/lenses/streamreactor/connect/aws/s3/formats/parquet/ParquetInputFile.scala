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
