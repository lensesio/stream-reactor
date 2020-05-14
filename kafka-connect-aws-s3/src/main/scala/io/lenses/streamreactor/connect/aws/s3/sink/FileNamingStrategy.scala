
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

import io.lenses.streamreactor.connect.aws.s3._
import io.lenses.streamreactor.connect.aws.s3.config.Format

import scala.util.Try

trait S3FileNamingStrategy {

  protected val DefaultPrefix = "streamreactor"

  def getFormat: Format

  def stagingFilename(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): BucketAndPath

  def finalFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset): BucketAndPath
}

class HierarchicalS3FileNamingStrategy(format: Format) extends S3FileNamingStrategy {

  override def stagingFilename(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${Prefix(bucketAndPrefix)}/.temp/${topicPartition.topic.value}/${topicPartition.partition}.${format.entryName.toLowerCase}")

  override def finalFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${Prefix(bucketAndPrefix)}/${topicPartitionOffset.topic.value}/${topicPartitionOffset.partition}/${topicPartitionOffset.offset.value}.${format.entryName.toLowerCase}")

  private def Prefix(bucketAndPrefix: BucketAndPrefix) = bucketAndPrefix.prefix.getOrElse(DefaultPrefix)

  override def getFormat: Format = format
}


object CommittedFileName {

  private val Regex = s"(.+)/(.+)/(\\d+)/(\\d+).(.+)".r

  def unapply(filename: String): Option[(String, Topic, Int, Offset, Format)] = {
    filename match {
      case Regex(prefix, topic, partition, end, extension) =>
        Format.withNameInsensitiveOption(extension) match {
          case Some(format) => Some(prefix, Topic(topic), partition.toInt, Offset(end.toLong), format)
          case None => None
        }

      case _ => None
    }
  }
}
