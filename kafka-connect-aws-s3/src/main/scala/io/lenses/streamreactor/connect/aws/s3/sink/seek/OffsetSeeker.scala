package io.lenses.streamreactor.connect.aws.s3.sink.seek

import io.lenses.streamreactor.connect.aws.s3.model.TopicPartition
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.sink.S3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.SinkError

trait OffsetSeeker {

  def seek(
    topicPartition:     TopicPartition,
    fileNamingStrategy: S3FileNamingStrategy,
    bucketAndPrefix:    RemoteS3RootLocation,
  ): Either[SinkError, Option[TopicPartitionOffset]]
}
