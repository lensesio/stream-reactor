package io.lenses.streamreactor.connect.aws.s3.storage

import java.io.File

trait UploadError {
  def message() : String
}

case class NonExistingFileError(file: File) extends UploadError {
  def message() = s"attempt to upload non-existing file (${file.getPath})"
}

case class ZeroByteFileError(file: File) extends UploadError {
  def message() = s"zero byte upload prevented (${file.getPath})"
}

case class UploadFailedError(exception: Throwable, file: File) extends UploadError {
  override def message(): String = s"upload error (${file.getPath}) ${exception.getMessage}"
}
