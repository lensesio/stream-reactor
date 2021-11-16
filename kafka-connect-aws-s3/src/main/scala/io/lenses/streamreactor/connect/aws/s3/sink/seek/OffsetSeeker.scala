package io.lenses.streamreactor.connect.aws.s3.sink.seek

import io.lenses.streamreactor.connect.aws.s3.model.{TopicPartition, TopicPartitionOffset}
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.sink.{S3FileNamingStrategy, SinkError}

trait OffsetSeeker {

  def seek(topicPartition: TopicPartition, fileNamingStrategy: S3FileNamingStrategy, bucketAndPrefix: RemoteS3RootLocation): Either[SinkError, Option[TopicPartitionOffset]]
}
