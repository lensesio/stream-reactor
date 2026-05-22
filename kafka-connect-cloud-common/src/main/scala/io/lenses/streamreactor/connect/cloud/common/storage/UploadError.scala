/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.storage

import cats.implicits.catsSyntaxOptionId

import java.io.File

trait UploadError {
  def message(): String

  def toExceptionOption: Option[Throwable] = Option.empty
}

case class NonExistingFileError(file: File) extends UploadError {
  override def message() = s"attempt to upload non-existing file (${file.getPath})"

}

case class ZeroByteFileError(file: File) extends UploadError {
  override def message() = s"zero byte upload prevented (${file.getPath})"
}

case class UploadFailedError(exception: Throwable, file: File) extends UploadError {
  override def message(): String = s"upload error (${file.getPath}) ${exception.getMessage}"

  override def toExceptionOption: Option[Throwable] = exception.some
}

case class EmptyContentsStringError(data: String) extends UploadError {
  override def message() = s"attempt to upload empty string (${data})"
}

case class FileMoveError(exception: Throwable, oldName: String, newName: String) extends UploadError {
  override def message() = s"error moving file from ($oldName) to ($newName) ${exception.getMessage}"

  override def toExceptionOption: Option[Throwable] = exception.some
}

case class FileCreateError(exception: Throwable, data: String) extends UploadError {
  override def message() = s"error writing file (${data}) ${exception.getMessage}"

  override def toExceptionOption: Option[Throwable] = exception.some
}

case class FileDeleteError(exception: Throwable, fileName: String) extends UploadError {
  override def message() = s"error deleting file ($fileName) ${exception.getMessage}"

  override def toExceptionOption: Option[Throwable] = exception.some
}

case class PathError(exception: Throwable, pathName: String) extends UploadError {
  override def message() = s"error loading file ($pathName) ${exception.getMessage}"
  def toException        = new RuntimeException(message(), exception)
  override def toExceptionOption: Option[Throwable] = exception.some
}

trait FileLoadError extends UploadError {
  override def message() = s"error loading file ($fileName) ${exception.getMessage}"

  val fileName:  String
  val exception: Throwable
  override def toExceptionOption: Option[Throwable] = exception.some
  def toException = new RuntimeException(message(), exception)

}

case class GeneralFileLoadError(exception: Throwable, fileName: String) extends FileLoadError
case class FileNotFoundError(exception: Throwable, fileName: String) extends FileLoadError

/**
 * Returned when a blob is read successfully but has zero content bytes.
 *
 * Unlike FileNotFoundError, callers MUST take a conditional-overwrite path (setIfMatch(eTag))
 * rather than a no-overwrite create path (setIfNoneMatch("*")), because the 0-byte file
 * already exists at the destination and a NoOverwrite write would always 412.
 *
 * The eTag is the load-bearing field: it allows the caller to issue an atomic
 * ObjectWithETag(idx, eTag) rename that takes ownership of the poison file.
 *
 * A length-0 blob cannot carry a committedOffset or PendingState by construction
 * (the serialiser produces at minimum ~40 bytes for any non-null IndexFile),
 * so treating it as recoverable is information-preserving.
 */
case class EmptyFileError(fileName: String, eTag: String) extends FileLoadError {
  override val exception: Throwable = new RuntimeException(s"Empty file at $fileName (eTag=$eTag)")
  override def message(): String    = s"empty lock file at $fileName (eTag=$eTag)"
}

case class FileNameParseError(exception: Throwable, fileName: String) extends UploadError {
  override def message() = s"error parsing file name ($fileName) ${exception.getMessage}"

  def toException = new RuntimeException(message(), exception)

  override def toExceptionOption: Option[Throwable] = exception.some
}
case class FileListError(exception: Throwable, bucket: String, path: Option[String]) extends UploadError {
  override def message() = s"error listing files ($path) ${exception.getMessage}"

  override def toExceptionOption: Option[Throwable] = exception.some

}

case class FileTouchError(exception: Throwable, fileName: String) extends UploadError {
  override def message() = s"error touching file ($fileName) ${exception.getMessage}"

  override def toExceptionOption: Option[Throwable] = exception.some
}
