package io.lenses.streamreactor.connect.aws.s3.storage

import java.io.ByteArrayOutputStream

class S3ByteArrayOutputStream extends S3OutputStream {

  val wrappedOutputStream = new ByteArrayOutputStream()

  var pointer : Long = 0L

  override def complete(): Boolean = true

  override def getPointer(): Long = pointer

  override def write(b: Int): Unit = {
    wrappedOutputStream.write(b)
    pointer += 1
  }

  def toByteArray() = wrappedOutputStream.toByteArray

  override def toString(): String = wrappedOutputStream.toString("UTF-8")
}
