
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

package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.Format
import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocation, RemoteS3RootLocation}
import io.lenses.streamreactor.connect.aws.s3.storage.{StorageInterface}

/**
  * The [[S3SourceLister]] is responsible for querying the [[StorageInterface]] to
  * retrieve a list of S3 topics and partitions for reading
  */
class S3SourceLister(format: Format)(implicit storageInterface: StorageInterface) extends LazyLogging {

  import storageInterface._

  def listBatch(
            bucketAndPrefix: RemoteS3RootLocation,
            lastFile: Option[RemoteS3PathLocation],
            numResults : Int
          ): Either[Throwable, List[RemoteS3PathLocation]] = {

    for {
      exists <- pathExists(bucketAndPrefix)
      list <- if (exists) list(bucketAndPrefix, lastFile, numResults) else List.empty[String].asRight
    } yield list.collect{
      case path: String if path.toLowerCase.endsWith(format.entryName.toLowerCase) => bucketAndPrefix.withPath(path)
    }

  }

}

