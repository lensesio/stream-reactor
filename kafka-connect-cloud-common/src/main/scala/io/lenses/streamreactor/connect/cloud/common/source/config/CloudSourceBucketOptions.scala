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
package io.lenses.streamreactor.connect.cloud.common.source.config

import cats.implicits.toTraverseOps
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.cloud.common.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.CloudSourceProps
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.CloudSourcePropsSchema
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

object CloudSourceBucketOptions {
  private val DEFAULT_RECORDS_LIMIT = 10000
  private val DEFAULT_FILES_LIMIT   = 1000

  def apply[M <: FileMetadata](
    config:             CloudSourceConfigDefBuilder,
    partitionExtractor: Option[PartitionExtractor],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, Seq[CloudSourceBucketOptions[M]]] =
    config.getKCQL.map {
      kcql: Kcql =>
        for {
          source     <- CloudLocation.splitAndValidate(kcql.getSource)
          format     <- FormatSelection.fromKcql(kcql, CloudSourcePropsSchema.schema)
          sourceProps = CloudSourceProps.fromKcql(kcql)

          //extract the envelope. of not present default to false
          hasEnvelope <- config.extractEnvelope(sourceProps)

        } yield CloudSourceBucketOptions[M](
          source,
          kcql.getTarget,
          format             = format,
          recordsLimit       = if (kcql.getLimit < 1) DEFAULT_RECORDS_LIMIT else kcql.getLimit,
          filesLimit         = if (kcql.getBatchSize < 1) DEFAULT_FILES_LIMIT else kcql.getBatchSize,
          partitionExtractor = partitionExtractor,
          orderingType       = config.extractOrderingType,
          hasEnvelope        = hasEnvelope.getOrElse(false),
        )
    }.toSeq.traverse(identity)

}

case class CloudSourceBucketOptions[M <: FileMetadata](
  sourceBucketAndPrefix: CloudLocation,
  targetTopic:           String,
  format:                FormatSelection,
  recordsLimit:          Int,
  filesLimit:            Int,
  partitionExtractor:    Option[PartitionExtractor],
  orderingType:          OrderingType,
  hasEnvelope:           Boolean,
) {
  def createBatchListerFn(
    storageInterface: StorageInterface[M],
  ): Option[M] => Either[FileListError, Option[ListOfKeysResponse[M]]] =
    orderingType
      .getBatchLister
      .listBatch(
        storageInterface = storageInterface,
        bucket           = sourceBucketAndPrefix.bucket,
        prefix           = sourceBucketAndPrefix.prefix,
        numResults       = filesLimit,
      )

  def getPartitionExtractorFn: String => Option[Int] =
    partitionExtractor.fold((_: String) => Option.empty[Int])(_.extract)

}
