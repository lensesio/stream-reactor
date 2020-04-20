package io.lenses.streamreactor.connect.aws.s3.storage

import java.io.OutputStream

trait S3OutputStream extends OutputStream {

  def complete(): Boolean

  def getPointer(): Long

}
