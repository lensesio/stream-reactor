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

import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

/**
  * As AWS's List results are always returned in UTF-8 binary order, the default mode of listing simply defers to Amazon to decide the order.
  */
object DefaultOrderingBatchLister extends BatchLister {

  override def listBatch[SM <: FileMetadata](
    storageInterface: StorageInterface[SM],
    bucket:           String,
    prefix:           Option[String],
    numResults:       Int,
  )(lastFile:         Option[SM],
  ): Either[FileListError, Option[ListOfKeysResponse[SM]]] =
    storageInterface
      .list(bucket, prefix, lastFile, numResults)
}
