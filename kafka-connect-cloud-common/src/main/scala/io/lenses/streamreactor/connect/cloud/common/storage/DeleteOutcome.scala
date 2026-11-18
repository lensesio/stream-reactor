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

/** One object to delete only while it is still at the version `eTag` names. */
final case class ConditionalDelete(path: String, eTag: String)

/**
 * The outcome of one conditional delete (`StorageInterface.deleteFilesIfMatch`).
 *
 * `PreconditionFailed` means the object is still there but at another version than the token the
 * caller observed: someone rewrote it since. It is a decision, not an error, so it is never retried.
 */
sealed trait DeleteOutcome

object DeleteOutcome {

  /** The object was at the given version and is gone now. */
  case object Deleted extends DeleteOutcome

  /** There was no object at the path. */
  case object NotFound extends DeleteOutcome

  /** The object exists at another version; nothing was deleted. */
  case object PreconditionFailed extends DeleteOutcome

  /** The delete failed for another reason (network, permissions, throttling); nothing is known. */
  final case class Failed(error: FileDeleteError) extends DeleteOutcome
}

/**
 * The cause a conditional `deleteFile` fails with when the object exists at another version than
 * the one given. Not transient: a retry would fail the same way.
 */
final class DeletePreconditionFailedException(val path: String, val eTag: String)
    extends RuntimeException(s"Delete of $path refused: the object is no longer at version $eTag")

object DeletePreconditionFailedException {

  /** The `deleteFile` result for one conditional delete outcome. */
  def toDeleteFileResult(path: String, eTag: String, outcome: DeleteOutcome): Either[FileDeleteError, Unit] =
    outcome match {
      case DeleteOutcome.Deleted | DeleteOutcome.NotFound => Right(())
      case DeleteOutcome.PreconditionFailed =>
        Left(FileDeleteError(new DeletePreconditionFailedException(path, eTag), path))
      case DeleteOutcome.Failed(err) => Left(err)
    }
}
