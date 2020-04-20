package io.lenses.streamreactor.connect.aws.s3.formats.parquet

import io.lenses.streamreactor.connect.aws.s3.storage.{MultipartBlobStoreOutputStream, S3OutputStream}
import org.apache.parquet.io.{OutputFile, PositionOutputStream}

class ParquetOutputFile(multipartBlobStoreOutputStream: S3OutputStream) extends OutputFile {

  override def create(blockSizeHint: Long): PositionOutputStream = new PositionOutputStreamWrapper(multipartBlobStoreOutputStream)

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = new PositionOutputStreamWrapper(multipartBlobStoreOutputStream)

  override def supportsBlockSize(): Boolean = false

  override def defaultBlockSize(): Long = 0

  class PositionOutputStreamWrapper(multipartBlobStoreOutputStream: S3OutputStream) extends PositionOutputStream {

    override def getPos: Long = multipartBlobStoreOutputStream.getPointer()

    override def write(b: Int): Unit = multipartBlobStoreOutputStream.write(b)

  }

}
