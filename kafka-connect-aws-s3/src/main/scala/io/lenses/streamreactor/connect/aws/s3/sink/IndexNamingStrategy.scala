package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, BucketAndPrefix, Offset, Topic, TopicPartition, TopicPartitionOffset}

import scala.util.matching.Regex

trait S3IndexNamingStrategy {

  def indexPath(bucket: String, topicPartition: TopicPartition): BucketAndPath

  def indexFilename(bucket: String, topicPartitionOffset: TopicPartitionOffset): BucketAndPath

  val indexFileNameRegex: Regex
}

class PartitionedS3IndexNamingStrategy extends S3IndexNamingStrategy {

  private val IndexPrefix = "index"

  def indexPath(bucket: String, topicPartition: TopicPartition): BucketAndPath =
    BucketAndPath(bucket, s"$IndexPrefix/${topicPartition.topic.value}/${topicPartition.partition}/")

  def indexFilename(bucket: String, topicPartitionOffset: TopicPartitionOffset): BucketAndPath =
    BucketAndPath(bucket, s"$IndexPrefix/${topicPartitionOffset.topic.value}/${topicPartitionOffset.partition}/latest.${topicPartitionOffset.offset.value}")

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
