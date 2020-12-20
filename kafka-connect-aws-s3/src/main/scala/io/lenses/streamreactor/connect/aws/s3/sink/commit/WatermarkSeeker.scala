package io.lenses.streamreactor.connect.aws.s3.sink.commit

import io.lenses.streamreactor.connect.aws.s3.config.CommitMode
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartition
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.Storage

/**
  * Obtains the latest offsets committed for a given topic-partition
  */
trait WatermarkSeeker {
  def latest(partitions: Set[TopicPartition]): Map[TopicPartition, Offset]
}

object WatermarkSeeker {
  def from(config:S3SinkConfig, storage:Storage):WatermarkSeeker={
    config.commitMode match {
      case CommitMode.Gen1 => Gen1Seeker.from(config, storage)
      case CommitMode.Gen2 =>
        //TODO: replace with Gen2
        Gen1Seeker.from(config, storage)
    }
  }
}