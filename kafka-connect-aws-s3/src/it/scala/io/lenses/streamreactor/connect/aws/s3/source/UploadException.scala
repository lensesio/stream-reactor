package io.lenses.streamreactor.connect.aws.s3.source

import io.lenses.streamreactor.connect.cloud.common.storage.UploadError

class UploadException(message: String, inner: Throwable) extends RuntimeException(message, inner) with UploadError {
  def this(throwable: Throwable)   = this(throwable.getMessage, throwable)
  def this(error:     UploadError) = this(error.message(), null)
  override def message(): String = message
}
