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
package io.lenses.streamreactor.connect.cloud.common.storage

/**
 * A class used to filter files based on their extensions.
 * It allows to include or exclude files with certain extensions.
 *
 * @constructor create a new ExtensionFilter with allowed and excluded extensions.
 * @param allowedExtensions the set of extensions that are allowed.
 * @param excludedExtensions the set of extensions that are excluded.
 */
class ExtensionFilter(
  val allowedExtensions:  Set[String],
  val excludedExtensions: Set[String],
) {

  /**
   * Filters the metadata of a file based on its extension.
   *
   * @param metadata the metadata of the file to be filtered.
   * @return true if the file passes the filter, false otherwise.
   */
  def filter[MD <: FileMetadata](metadata: MD): Boolean =
    ExtensionFilter.performFilterLogic(metadata.file.toLowerCase, allowedExtensions, excludedExtensions)

  /**
   * Filters a file based on its name.
   *
   * @param fileName the name of the file to be filtered.
   * @return true if the file passes the filter, false otherwise.
   */
  def filter(fileName: String): Boolean =
    ExtensionFilter.performFilterLogic(fileName.toLowerCase, allowedExtensions, excludedExtensions)

}

object ExtensionFilter {

  def performFilterLogic(
    fileName:           String,
    allowedExtensions:  Set[String],
    excludedExtensions: Set[String],
  ): Boolean = {
    val allowedContainsEx     = allowedExtensions.exists(ext => fileName.endsWith(ext))
    val excludedNotContainsEx = excludedExtensions.forall(ext => !fileName.endsWith(ext))
    (allowedExtensions.isEmpty || allowedContainsEx) && excludedNotContainsEx
  }

}
