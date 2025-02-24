/*
 * Copyright 2017-2025 Lenses.io Ltd
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

trait ObjectProtection[O] {
  def wrappedObject: O
}

case class NonOverwriteProtected[O](wrappedObject: O) extends ObjectProtection[O]

case class NoOverwriteExistingObject[O](wrappedObject: O) extends ObjectProtection[O]

case class ObjectWithETag[O](
  wrappedObject: O,
  eTag:          String,
) extends ObjectProtection[O]

//case class OffsetRange(from: Option[Offset], to: Option[Offset])

trait FileOperation
case class UploadOperation(bucket: String, source: File, destination: String) extends FileOperation
case class CopyOperation(bucket: String, source: String, destination: String, eTag: String) extends FileOperation
case class DeleteOperation(bucket: String, source: String, eTag: String) extends FileOperation

case class PendingState(
  pendingOffset:     Offset,
  pendingOperations: NonEmptyList[FileOperation],
)

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

  //implicit val offsetRangeEncoder: Encoder[OffsetRange] = deriveEncoder
  //implicit val offsetRangeDecoder: Decoder[OffsetRange] = deriveDecoder

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
