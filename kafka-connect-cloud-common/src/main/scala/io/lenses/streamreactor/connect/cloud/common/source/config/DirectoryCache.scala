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
package io.lenses.streamreactor.connect.cloud.common.source.config

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

import scala.collection.mutable

/**
  * A cache for tracking directories that have been created.
  *
  * @param storageInterface The storage interface used to create directories.
  */
class DirectoryCache(storageInterface: StorageInterface[_]) {

  // A mutable set to keep track of created directories.
  private val directoriesCreated = mutable.Set[(String, String)]()

  /**
    * Ensures that a directory exists in the specified bucket and path.
    *
    * @param bucket The bucket in which the directory should exist.
    * @param path The path of the directory to check or create.
    * @return Either a FileCreateError if the directory creation failed, or Unit if the directory exists or was created successfully.
    */
  def ensureDirectoryExists(bucket: String, path: String): Either[FileCreateError, Unit] =
    if (directoriesCreated.contains((bucket, path))) {
      ().asRight
    } else {
      storageInterface.createDirectoryIfNotExists(bucket, path) match {
        case Left(value) => value.asLeft
        case Right(_) =>
          directoriesCreated.add((bucket, path))
          ().asRight
      }
    }

}
