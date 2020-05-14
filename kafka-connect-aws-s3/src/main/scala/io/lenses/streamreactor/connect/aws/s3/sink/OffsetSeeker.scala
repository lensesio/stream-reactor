
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

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.{BucketAndPrefix, TopicPartitionOffset}

import scala.util.control.NonFatal

/**
  * The [[OffsetSeeker]] is responsible for querying the [[StorageInterface]] to
  * retrieve current offset information from a container.
  *
  * @param fileNamingStrategy we need the policy so we can match on this.
  */
class OffsetSeeker(fileNamingStrategy: S3FileNamingStrategy) {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  def seek(bucketAndPrefix: BucketAndPrefix)(implicit storageInterface: StorageInterface): Set[TopicPartitionOffset] = {
    try {

      // the path may not have been created, in which case we have no offsets defined
      if (storageInterface.pathExists(bucketAndPrefix)) {

        val listOfFilesInBucket = storageInterface.list(bucketAndPrefix)

        listOfFilesInBucket.collect {
          case CommittedFileName(_, topic, partition, end, format)
            if format == fileNamingStrategy.getFormat =>
            TopicPartitionOffset(topic, partition, end)
        }.groupBy(_.toTopicPartition).map { case (tp, tpo) =>
          tp.withOffset(tpo.maxBy(_.offset.value).offset)
        }.toSet

      } else {
        Set.empty
      }

    } catch {
      case NonFatal(e) =>
        logger.error(s"Error seeking bucket/prefix $bucketAndPrefix")
        throw e
    }

  }

}

