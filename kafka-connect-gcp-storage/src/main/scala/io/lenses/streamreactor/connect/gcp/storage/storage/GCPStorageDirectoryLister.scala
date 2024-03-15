/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.storage.storage

import cats.effect.IO
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryLister

import scala.jdk.CollectionConverters.IterableHasAsScala

class GCPStorageDirectoryLister(connectorTaskId: ConnectorTaskId, storage: Storage)
    extends LazyLogging
    with DirectoryLister {

  /**
    * Finds directories within the specified cloud location with optional recursion.
    *
    * This method searches for directories within the given cloud location (bucket and prefix) up to a certain recursion level.
    * Directories matching the specified criteria (limits, exclusions) are returned as a set of strings.
    *
    * @param bucketAndPrefix The cloud location consisting of a bucket and prefix.
    * @param filesLimit The maximum number of files to list in each directory.
    * @param recurseLevels The maximum recursion levels to search within directories.
    * @param exclude The set of directory prefixes to exclude from the search.  We ignore paths containing certain strings.  Mainly it is used to prevent us from reading anything inside the .indexes key prefix, as these should be ignored by the source.
    * @param wildcardExcludes The set of wildcard patterns to exclude from the search.
    * @return An IO containing a set of directory paths found within the specified cloud location.
    */
  override def findDirectories(
    bucketAndPrefix:  CloudLocation,
    filesLimit:       Int,
    recurseLevels:    Int,
    exclude:          Set[String],
    wildcardExcludes: Set[String],
  ): IO[Set[String]] = {

    /**
      * Recursively lists subdirectories within the specified cloud location prefix up to a certain recursion level.
      *
      * This method iterates through the subdirectories of the given prefix within a cloud location, up to the specified recursion level.
      * It filters the results based on ownership, exclusions, and wildcard patterns.
      *
      * @param prefix        The prefix within the cloud location to search for subdirectories.
      * @param recurseLevels The maximum recursion levels to search within subdirectories.
      * @return An iterable collection of subdirectory paths found within the specified prefix with the indicated recursion level.
      */
    def listSubdirs(prefix: String, recurseLevels: Int): Iterable[String] = {
      val blobListOptions = BlobListOption.dedupe(
        BlobListOption.delimiter("/"),
        BlobListOption.pageSize(filesLimit.toLong),
        BlobListOption.prefix(prefix),
        BlobListOption.currentDirectory(),
      )

      val foundResults = storage
        .get(bucketAndPrefix.bucket)
        .list(blobListOptions: _*)
        .iterateAll()
        .asScala
        .filter(_.isDirectory)
        .map(_.getName)
        .toList
        .filter { prefix =>
          connectorTaskId.ownsDir(prefix) && !exclude.contains(prefix) && !wildcardExcludes.exists(we =>
            prefix.contains(we),
          )
        }

      logger.trace(s"[$connectorTaskId] Searching directory $prefix for $recurseLevels, found ${foundResults.size}")

      foundResults.flatMap {
        case d: String if recurseLevels > 1 =>
          listSubdirs(d, recurseLevels - 1)
        case _ =>
          foundResults
      }

    }

    val preWithTrailingSlash: String = ensureTrailingSlash(bucketAndPrefix.prefixOrDefault())

    if (recurseLevels == 0) {
      IO(Set(preWithTrailingSlash))
    } else {
      for {
        iterator <- IO(listSubdirs(preWithTrailingSlash, recurseLevels))
      } yield iterator.toSet
    }
  }

  /**
    * Ensures that the given string ends with a trailing slash ("/").
    *
    * If the input string is empty or already ends with a slash, the same string is returned.
    * Otherwise, a slash is appended to the end of the string.
    *
    * Note: An empty string is a valid path at the root of the GCP storage bucket.  A trailing slash would not
    * be appropriate.
    *
    * @param pre The input string to ensure a trailing slash for.
    * @return The input string with a trailing slash appended if necessary.
    */
  private def ensureTrailingSlash(pre: String): String =
    if (pre.isEmpty || pre.endsWith("/")) pre else s"$pre/"
}
