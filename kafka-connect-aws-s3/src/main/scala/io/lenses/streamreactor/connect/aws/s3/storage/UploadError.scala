package io.lenses.streamreactor.connect.aws.s3.storage

import java.io.File

trait UploadError {
  def message(): String
}

case class NonExistingFileError(file: File) extends UploadError {
  override def message() = s"attempt to upload non-existing file (${file.getPath})"
}

case class ZeroByteFileError(file: File) extends UploadError {
  override def message() = s"zero byte upload prevented (${file.getPath})"
}

case class UploadFailedError(exception: Throwable, file: File) extends UploadError {
  override def message(): String = s"upload error (${file.getPath}) ${exception.getMessage}"
}

case class EmptyContentsStringError(data: String) extends UploadError {
  override def message() = s"attempt to upload empty string (${data})"
}

case class FileCreateError(exception: Throwable, data: String) extends UploadError {
  override def message() = s"error writing file (${data}) ${exception.getMessage}"
}

case class FileDeleteError(exception: Throwable, fileName: String) extends UploadError {
  override def message() = s"error deleting file (${fileName}) ${exception.getMessage}"
}

case class FileLoadError(exception: Throwable, fileName: String) extends UploadError {
  override def message() = s"error loading file (${fileName}) ${exception.getMessage}"
}

case class FileListError(exception: Throwable, path: String) extends UploadError {
  override def message() = s"error listing files (${path}) ${exception.getMessage}"
}
