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
package io.lenses.streamreactor.connect.cloud.common.source.files

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

/**
  * If an order other than UTF-8 binary order is desired, then this can only be achieved by downloading all the results from the bucket and ordering them in memory.  It is recommended to be able to use the default ordering wherever possible, but if you need to sort by file modification date then use this.
  */
object DateOrderingBatchLister extends BatchLister {
  override def listBatch[SM <: FileMetadata](
    storageInterface: StorageInterface[SM],
    bucket:           String,
    prefix:           Option[String],
    numResults:       Int,
  )(lastFile:         Option[SM],
  ): Either[FileListError, Option[ListOfKeysResponse[SM]]] = {
    val lastFileFilter = filter(lastFile) _

    for {
      listResp <- storageInterface.listFileMetaRecursive(bucket, prefix)
      ordered   = listResp.iterator.flatMap(_.files).filter(lastFileFilter).toSeq.sortBy(_.lastModified).take(numResults)
    } yield {
      ordered.lastOption.flatMap(lo => ListOfKeysResponse[SM](bucket, prefix, ordered.map(_.file), lo).some)
    }
  }

  private def filter(maybeLastFile: Option[FileMetadata])(thisFile: FileMetadata): Boolean =
    maybeLastFile.fold(true) {
      lastFile =>
        thisFile.lastModified.isAfter(lastFile.lastModified) &&
        thisFile.file != lastFile.file
    }

}
