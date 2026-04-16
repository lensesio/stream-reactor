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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.data.NonEmptyList
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax.EncoderOps
import io.lenses.streamreactor.connect.cloud.common.model.Offset

import java.io.File

/**
 * A trait representing an object wrapper that provides protection (on the cloud storage layer) for operations on the wrapped object.
 *
 * @tparam O The type of the object being wrapped.
 */
trait ObjectProtection[O] {

  /**
   * The object being wrapped.
   *
   * @return The wrapped object of type `O`.
   */
  def wrappedObject: O
}

/**
 * A case class representing an object that should not be overwritten if it already exists.
 *
 * @param wrappedObject The object being protected from overwriting.
 * @tparam O The type of the object being wrapped.
 */
case class NoOverwriteExistingObject[O](wrappedObject: O) extends ObjectProtection[O]

/**
 * A case class representing an object with an associated EeTagTag for versioning or identification.
 * When the eTag is defined the object cannot be overwritten unless the eTag matches the
 * existent version of the object.
 *
 * @param wrappedObject The object being wrapped.
 * @param eTag A string representing the eTag associated with the object.
 * @tparam O The type of the object being wrapped.
 */
case class ObjectWithETag[O](
  wrappedObject: O,
  eTag:          String,
) extends ObjectProtection[O]

/**
 * A sealed trait representing a file operation.
 * This serves as a base type for specific file operations such as upload, copy, and delete.
 */
trait FileOperation

/**
 * Represents an operation to upload a file to a specific bucket and destination.
 *
 * @param bucket      The name of the bucket where the file will be uploaded.
 * @param source      The source file to be uploaded.
 * @param destination The destination path within the bucket.
 */
case class UploadOperation(bucket: String, source: File, destination: String) extends FileOperation

/**
 * Represents an operation to copy a file within a bucket.
 *
 * @param bucket      The name of the bucket where the file resides.
 * @param source      The source path of the file to be copied.
 * @param destination The destination path where the file will be copied.
 * @param eTag        The ETag of the source file, used for versioning or validation.
 */
case class CopyOperation(bucket: String, source: String, destination: String, eTag: String) extends FileOperation

/**
 * Represents an operation to delete a file from a bucket.
 *
 * @param bucket The name of the bucket where the file resides.
 * @param source The path of the file to be deleted.
 * @param eTag   The ETag of the file, used for versioning or validation.
 */
case class DeleteOperation(bucket: String, source: String, eTag: String) extends FileOperation

/**
 * Represents the state of pending operations for a specific offset.
 *
 * @param pendingOffset     The offset associated with the pending operations.
 * @param pendingOperations A non-empty list of file operations that are pending.
 */
case class PendingState(
  pendingOffset:     Offset,
  pendingOperations: NonEmptyList[FileOperation],
)

/**
 * Represents an index file containing metadata about the owner, committed offset,
 * and any pending state.
 *
 * @param owner           The owner of the index file.
 * @param committedOffset The last committed offset, if available.
 * @param pendingState    The optional state of pending operations, if any.
 */
case class IndexFile(
  owner:           String,
  committedOffset: Option[Offset],
  pendingState:    Option[PendingState],
)

object IndexFile {

  implicit val fileEncoder: Encoder[File] = (a: File) => Json.fromString(a.getPath)

  implicit val fileDecoder: Decoder[File] = (c: HCursor) =>
    for {
      value <- c.as[String]
    } yield new File(value)
  implicit val offsetEncoder: Encoder[Offset] = (a: Offset) => Json.fromLong(a.value)

  implicit val offsetDecoder: Decoder[Offset] = (c: HCursor) =>
    for {
      value <- c.as[Long]
    } yield Offset(value)

  implicit val deleteOperationEncoder: Encoder[DeleteOperation] = deriveEncoder
  implicit val deleteOperationDecoder: Decoder[DeleteOperation] = deriveDecoder

  implicit val copyOperationEncoder: Encoder[CopyOperation] = deriveEncoder
  implicit val copyOperationDecoder: Decoder[CopyOperation] = deriveDecoder

  implicit val uploadOperationEncoder: Encoder[UploadOperation] = deriveEncoder
  implicit val uploadOperationDecoder: Decoder[UploadOperation] = deriveDecoder
  implicit val fileOperationEncoder: Encoder[FileOperation] = Encoder.instance {
    case copy:   CopyOperation   => copy.asJson.deepMerge(Json.obj("type" -> Json.fromString("copy")))
    case delete: DeleteOperation => delete.asJson.deepMerge(Json.obj("type" -> Json.fromString("delete")))
    case upload: UploadOperation => upload.asJson.deepMerge(Json.obj("type" -> Json.fromString("upload")))
  }

  implicit val fileOperationDecoder: Decoder[FileOperation] = Decoder.instance { cursor =>
    cursor.get[String]("type").flatMap {
      case "upload" => cursor.as[UploadOperation]
      case "copy"   => cursor.as[CopyOperation]
      case "delete" => cursor.as[DeleteOperation]
      case other    => Left(DecodingFailure(s"Unknown type: $other", cursor.history))
    }
  }

  implicit val indexFileEncoder: Encoder[IndexFile] = deriveEncoder
  implicit val indexFileDecoder: Decoder[IndexFile] = deriveDecoder

  implicit val pendingStateEncoder: Encoder[PendingState] = deriveEncoder
  implicit val pendingStateDecoder: Decoder[PendingState] = deriveDecoder
}
