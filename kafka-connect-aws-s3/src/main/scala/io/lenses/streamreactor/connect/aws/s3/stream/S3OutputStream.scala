package io.lenses.streamreactor.connect.aws.s3.stream

import io.lenses.streamreactor.connect.aws.s3.sink.SinkError

import java.io.OutputStream

trait S3OutputStream extends OutputStream {

  def complete(): Either[SinkError, Unit]

  def getPointer: Long

}
