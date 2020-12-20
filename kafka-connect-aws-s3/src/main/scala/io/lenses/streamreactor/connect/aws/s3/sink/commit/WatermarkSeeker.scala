package io.lenses.streamreactor.connect.aws.s3.sink.commit

import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartition

/**
  * Obtains the latest offsets committed for a given topic-partition
  */
trait WatermarkSeeker {
  def latest(partitions: Set[TopicPartition]): Map[TopicPartition, Offset]
}
