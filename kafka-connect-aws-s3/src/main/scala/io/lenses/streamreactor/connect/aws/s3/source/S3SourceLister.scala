
/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.source

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPrefix, S3StoredFile, S3StoredFileSorter}
import io.lenses.streamreactor.connect.aws.s3.sink.S3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

import scala.util.control.NonFatal

/**
  * The [[S3SourceLister]] is responsible for querying the [[StorageInterface]] to
  * retrieve a list of S3 topics and partitions for reading
  */
class S3SourceLister(implicit storageInterface: StorageInterface) extends LazyLogging {

  def list(implicit fileNamingStrategy: S3FileNamingStrategy, bucketAndPrefix: BucketAndPrefix): List[S3StoredFile] = {

    def bucketLocationExists: Boolean = storageInterface.pathExists(bucketAndPrefix)

    def listFilesInS3: List[S3StoredFile] = storageInterface
      .list(bucketAndPrefix)
      .flatMap(S3StoredFile(_))

    try {

      // the path may not have been created, in which case we have no offsets defined
      if (bucketLocationExists) S3StoredFileSorter.sort(listFilesInS3) else List.empty

    } catch {
      case NonFatal(e) =>
        logger.error(s"Error listing bucket/prefix $bucketAndPrefix")
        throw e
    }

  }

  def next(fileNamingStrategy: S3FileNamingStrategy, bucketAndPrefix: BucketAndPrefix, lastResult: Option[S3StoredFile], resumeFrom: Option[S3StoredFile]): Option[S3StoredFile] = {
    val names = list(fileNamingStrategy, bucketAndPrefix)

    val position = if (resumeFrom.nonEmpty) {
      resumeFrom.fold(0) { result =>
        val idx = names.indexOf(result)
        if (idx == -1) {
          throw new IllegalStateException(s"${result.path} was not found in $names")
        }
        idx
      }
    } else {
      lastResult.fold(0) { result =>
        names.indexOf(result) + 1
      }
    }

    if (names.nonEmpty && position < names.size) Some(names(position)) else None

  }

}

