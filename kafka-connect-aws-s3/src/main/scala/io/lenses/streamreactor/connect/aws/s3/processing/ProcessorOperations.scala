/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.processing

import io.lenses.streamreactor.connect.aws.s3.model.{LocalLocation, Offset, RemotePathLocation}
import io.lenses.streamreactor.connect.aws.s3.sink.ProcessorException
import io.lenses.streamreactor.connect.aws.s3.storage.{MultiPartUploadState, StorageInterface}

import scala.util.Try

sealed abstract class ProcessorOperation(maybeOffset: Option[Offset]) {
  def process(implicit storageInterface: StorageInterface): Either[ProcessorException, Unit]

  private val offset: String = maybeOffset.mkString

  override def toString = s"$offset | "
}

case class InitUploadProcessorOperation(initialOffset: Offset, initialName: RemotePathLocation, callbackFn: MultiPartUploadState => Unit) extends ProcessorOperation(Some(initialOffset)) {
  override def toString = super.toString + s"initUpload ($initialName)"

  override def process(implicit storageInterface: StorageInterface): Either[ProcessorException, Unit] = {
    Try {
      callbackFn(storageInterface.initUpload(initialName))
    }.toEither.left.map(ex => ProcessorException(ex))
  }
}

case class CompleteUploadProcessorOperation(offset: Offset, stateFn: () => MultiPartUploadState) extends ProcessorOperation(Some(offset)) {
  override def toString: String = super.toString + s"completeUpload (latestState)"

  override def process(implicit storageInterface: StorageInterface): Either[ProcessorException, Unit] = Try {
    stateFn() match {
      case state@MultiPartUploadState(_, parts) if parts.nonEmpty => storageInterface.completeUpload(state)
      case _ =>
    }
  }.toEither.left.map(ex => ProcessorException(ex))
}

case class UploadPartProcessorOperation(maybeOffset: Option[Offset], stateFn: () => MultiPartUploadState, bytes: Array[Byte], callbackFn: MultiPartUploadState => Unit) extends ProcessorOperation(maybeOffset) {
  override def toString: String = super.toString + s"uploadPart (latestState)"

  override def process(implicit storageInterface: StorageInterface): Either[ProcessorException, Unit] = Try {
    callbackFn(storageInterface.uploadPart(stateFn(), bytes))
  }.toEither.left.map(ex => ProcessorException(ex))
}

// completion functions

case class UploadFileProcessorOperation(offset: Offset, initialName: LocalLocation, finalDestination: RemotePathLocation, callbackFn: () => Unit) extends ProcessorOperation(Some(offset)) {
  override def toString = super.toString + s"uploadFile ($initialName -> $finalDestination)"

  override def process(implicit storageInterface: StorageInterface): Either[ProcessorException, Unit] = Try {
    storageInterface.uploadFile(initialName, finalDestination)
    callbackFn()
  }.toEither.left.map(ex => ProcessorException(ex))
}

case class RenameFileProcessorOperation(offset: Offset, originalFilename: RemotePathLocation, newFilename: RemotePathLocation, callbackFn: () => Unit) extends ProcessorOperation(Some(offset)) {
  override def toString = super.toString + s"renameFile ($originalFilename -> $newFilename)"

  override def process(implicit storageInterface: StorageInterface): Either[ProcessorException, Unit] = Try {
    storageInterface.rename(originalFilename, newFilename)
    callbackFn()
  }.toEither.left.map(ex => ProcessorException(ex))
}
