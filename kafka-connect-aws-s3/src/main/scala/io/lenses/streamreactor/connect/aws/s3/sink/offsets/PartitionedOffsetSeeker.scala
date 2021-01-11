
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

import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.{CommittedFileName, S3FileNamingStrategy}
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

import scala.util.control.NonFatal

/**
  * The [[PartitionedOffsetSeeker]] is responsible for querying the [[StorageInterface]] to
  * retrieve current offset information from a container.
  *
  * @param fileNamingStrategy we need the policy so we can match on this.
  */
class PartitionedOffsetSeeker(storageInterface: StorageInterface)(implicit fileNamingStrategy: S3FileNamingStrategy) extends OffsetSeeker {
  
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def seek(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): Option[(BucketAndPath,TopicPartitionOffset)] = {

    try {

      val bucketAndPath = bucketAndPrefix.toPath
      // the path may not have been created, in which case we have no offsets defined
      if (storageInterface.pathExists(bucketAndPath)) {
        extractLatestOffsets(bucketAndPrefix, topicPartition)
          .map{
            case (path, topicPartitionOffset) => (BucketAndPath(bucketAndPrefix.bucket, path), topicPartitionOffset)
          }
      } else {
        None
      }

    } catch {
      case NonFatal(e) =>
        logger.error(s"Error seeking bucket/prefix $bucketAndPrefix")
        throw e
    }

  }


  private def extractLatestOffsets(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): Option[(String, TopicPartitionOffset)] = {
    
    val latestEligible: List[(String, TopicPartitionOffset)] = storageInterface
      .listUsingStringMatching(
        bucketAndPrefix.toPath,
        fileNamingStrategy.getFormat
      )
      .collect {
        case value@CommittedFileName(topic, partition, end, format)
          if format == fileNamingStrategy.getFormat &&
            topic == topicPartition.topic &&
            partition == topicPartition.partition =>
          (value, TopicPartitionOffset(topic, partition, end))
      }
    
    if(latestEligible.isEmpty) {
      storageInterface.list(bucketAndPrefix.toPath)
        .collect {
          case value@CommittedFileName(topic, partition, end, format)
            if format == fileNamingStrategy.getFormat &&
              topic == topicPartition.topic &&
              partition == topicPartition.partition =>
            (value, TopicPartitionOffset(topic, partition, end))
        }.sortBy{
          case (offset, _) => offset
        }.headOption
    } else if (latestEligible.size == 1) {
      latestEligible.headOption
    }  else {
      cleanUp(bucketAndPrefix, latestEligible)
    }
  }

  private def cleanUp(bucketAndPrefix: BucketAndPrefix, latestEligible: List[(String, TopicPartitionOffset)]): Option[(String, TopicPartitionOffset)] = {
    if (latestEligible.size > 5) {
      logger.warn("Large number of latest eligible marked files, could indicate an issue with the sink")
    }

    val eligible = latestEligible
      .sortBy {
        case (_, tpo) => tpo.offset
      }
      .reverse

    eligible
      .tail
      .foreach(
        file => {
          val originalName = BucketAndPath(bucketAndPrefix.bucket, file._1)
          storageInterface.rename(
            originalName,
            fileNamingStrategy.convertLatestToFinalFilename(originalName)
          )
        }
      )
    
    Some(eligible.head)
  }

}

