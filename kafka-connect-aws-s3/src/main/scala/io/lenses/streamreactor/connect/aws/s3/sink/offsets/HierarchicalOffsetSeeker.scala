
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

package io.lenses.streamreactor.connect.aws.s3.sink.offsets

import io.lenses.streamreactor.connect.aws.s3.model.Offset.orderingByOffsetValue
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.{CommittedFileName, OffsetSeeker, S3FileNamingStrategy}
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

import scala.util.control.NonFatal

/**
  * The [[HierarchicalOffsetSeeker]] is responsible for querying the [[StorageInterface]] to
  * retrieve current offset information from a container.
  *
  * @param fileNamingStrategy we need the policy so we can match on this.
  */
class HierarchicalOffsetSeeker(implicit fileNamingStrategy: S3FileNamingStrategy, storageInterface: StorageInterface) extends OffsetSeeker {
  
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def seek(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): Option[(BucketAndPath,TopicPartitionOffset)] = {

    val parentBucketAndPath = fileNamingStrategy.topicPartitionPrefix(bucketAndPrefix, topicPartition)
    val latestBucketAndPath = fileNamingStrategy.topicPartitionPrefixLatest(bucketAndPrefix, topicPartition)
    try {

      // the path may not have been created, in which case we have no offsets defined
      if (storageInterface.pathExists(parentBucketAndPath)) {
        extractLatestOffsets(bucketAndPrefix, parentBucketAndPath, latestBucketAndPath)
          .map(e => (BucketAndPath(bucketAndPrefix.bucket, e._1), e._2))
      } else {
        None
      }

    } catch {
      case NonFatal(e) =>
        logger.error(s"Error seeking bucket/prefix $parentBucketAndPath /  $latestBucketAndPath")
        throw e
    }

  }

  private def extractLatestOffsets(bucketAndPrefix: BucketAndPrefix, parentBucketAndPath: BucketAndPath, latestBucketAndPath: BucketAndPath): Option[(String,TopicPartitionOffset)] = {
    val latestEligible: List[String] = storageInterface.list(latestBucketAndPath)

    val latestResult: Option[String] = if (latestEligible.isEmpty) {
      storageInterface.fetchSingleLatestUsingLastModified(parentBucketAndPath, fileNamingStrategy.getFormat)

    } else if (latestEligible.size == 1) {
      // no cleanup needed, just build the CommittedFileName
      latestEligible.headOption

    } else {
      // parse, cleanup, then return the committed filename
      cleanUp(bucketAndPrefix, latestEligible)
    }

    latestResult.map {
      case value @ CommittedFileName(topic, partition, end, format)
        if format == fileNamingStrategy.getFormat =>
        (value,TopicPartitionOffset(topic, partition, end))
    }
  }

  private def cleanUp(bucketAndPrefix: BucketAndPrefix, latestEligible: List[String]): Option[String] = {
    if(latestEligible.size > 5) {
      logger.warn("Large number of latest eligible marked files, could indicate an issue with the sink")
    }
    val eligible = latestEligible.collect{
      case value@CommittedFileName(_, _, offset, format) if format == fileNamingStrategy.getFormat =>
        (offset, BucketAndPath(bucketAndPrefix.bucket, value))
    }.sortBy(_._1).reverse

    eligible.tail.foreach(file => {
      storageInterface.rename(
        file._2,
        fileNamingStrategy.convertLatestToFinalFilename(file._2)
      )
    })
    Some(eligible.head._2.path)
  }

}

