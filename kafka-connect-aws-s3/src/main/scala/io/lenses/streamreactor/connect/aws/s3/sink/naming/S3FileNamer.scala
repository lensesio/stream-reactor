package io.lenses.streamreactor.connect.aws.s3.sink.naming

import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.aws.s3.sink.NoOpPaddingStrategy.padString

trait S3FileNamer {
  def fileName(formatSelection: FormatSelection, topicPartitionOffset: TopicPartitionOffset): String
}
object HierarchicalS3FileNamer extends S3FileNamer {
  def fileName(formatSelection: FormatSelection, topicPartitionOffset: TopicPartitionOffset): String = {
    s"/${padString(topicPartitionOffset.offset.value.toString)}.${formatSelection.extension}"
  }
}
object PartitionedS3FileNamer extends S3FileNamer {
  def fileName(formatSelection: FormatSelection, topicPartitionOffset: TopicPartitionOffset):  String = {

      s"${topicPartitionOffset.topic.value}(${
          padString(
            topicPartitionOffset.partition.toString,
          )
        }_${padString(topicPartitionOffset.offset.value.toString)}).${formatSelection.extension}"
  }


}
