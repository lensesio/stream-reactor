/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.source.files
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.storage.ResultProcessors.processObjectsAsFileMeta
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.FileMetadata
import io.lenses.streamreactor.connect.aws.s3.storage.ListResponse
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

/**
  * If an order other than UTF-8 binary order is desired, then this can only be achieved by downloading all the results from the bucket and ordering them in memory.  It is recommended to be able to use the default ordering wherever possible, but if you need to sort by file modification date then use this.
  */
object DateOrderingBatchLister extends BatchLister {
  override def listBatch(
    storageInterface: StorageInterface,
    bucketAndPrefix:  RemoteS3PathLocation,
    numResults:       Int,
  )(lastFile:         Option[FileMetadata],
  ): Either[FileListError, Option[ListResponse[String]]] = {
    val lastFileFilter = filter(lastFile) _
    for {
      listResp <- storageInterface.listRecursive(bucketAndPrefix, processObjectsAsFileMeta)
      ordered   = listResp.iterator.flatMap(_.files).filter(lastFileFilter).toSeq.sortBy(_.lastModified).take(numResults)
    } yield {
      ordered.lastOption.fold(Option.empty[ListResponse[String]])((last: FileMetadata) =>
        ListResponse[String](bucketAndPrefix, ordered.map(_.file), last).some,
      )
    }
  }

  private def filter(maybeLastFile: Option[FileMetadata])(thisFile: FileMetadata): Boolean =
    maybeLastFile.fold(true) {
      lastFile =>
        thisFile.lastModified.isAfter(lastFile.lastModified) &&
        thisFile.file != lastFile.file
    }

}
