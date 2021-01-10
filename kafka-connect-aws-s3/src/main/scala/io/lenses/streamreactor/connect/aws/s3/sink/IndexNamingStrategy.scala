package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, BucketAndPrefix, Offset, Topic, TopicPartition, TopicPartitionOffset}

import scala.util.matching.Regex

class S3IndexNamingStrategy {

  protected val DefaultPrefix = "index"

  def prefix(bucketAndPrefix: BucketAndPrefix): String = bucketAndPrefix.prefix.getOrElse(DefaultPrefix)

  def indexPath(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${prefix(bucketAndPrefix)}/${topicPartition.topic.value}/${topicPartition.partition}/")

  def indexFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${prefix(bucketAndPrefix)}/${topicPartitionOffset.topic.value}/${topicPartitionOffset.partition}/latest.${topicPartitionOffset.offset.value}")

  val indexFileNameRegex: Regex = s".+/(.+)/(\\d+)/latest.(\\d+)".r
}

case class IndexFileName(topic:Topic, partition:Int, offset:Offset)

object IndexFileName {
  def from(filename: String, s3IndexNamingStrategy: S3IndexNamingStrategy): Option[IndexFileName] = {
    filename match {
      case s3IndexNamingStrategy.indexFileNameRegex(topic, partition, latestOffset) =>
        Some(IndexFileName(Topic(topic), partition.toInt, Offset(latestOffset.toLong)))
      case _ => None
    }
  }
}
