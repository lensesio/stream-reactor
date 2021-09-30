package io.lenses.streamreactor.connect.aws.s3.storage

trait UploadError {
  def message() : String
}

case class FatalNonExistingFileError(filePath: String) extends UploadError {
  def message = s"attempt to upload non-existing file ($filePath)"
}

case class FatalZeroByteFileError(filePath: String) extends UploadError {
  def message = s"zero byte upload prevented ($filePath)"
}

case class UploadFailedError(exception: Throwable, filePath: String) extends UploadError {
  override def message(): String = s"upload error ($filePath) ${exception.getMessage}"
}
