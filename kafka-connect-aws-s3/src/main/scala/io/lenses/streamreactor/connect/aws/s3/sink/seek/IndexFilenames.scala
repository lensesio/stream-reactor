package io.lenses.streamreactor.connect.aws.s3.sink.seek

import io.lenses.streamreactor.connect.aws.s3.model.Offset

import scala.util.Try

object IndexFilenames {

  /**
   * Generate the filename for the index file.
   */
  def indexFilename(sinkName: String, topic: String, partition: Int, offset: Long) : String = {
     f"${indexForTopicPartition(sinkName, topic, partition)}$offset%020d"
  }

  /**
   * Generate the directory of the index for a given topic and partition
   */
  def indexForTopicPartition(sinkName: String, topic: String, partition: Int) : String = {
    f".indexes/$sinkName/$topic/$partition%05d/"
  }

  /**
   * Parses an index filename and returns an offset
   */
  def offsetFromIndex(indexFilename: String) : Either[Throwable, Offset] = {
    val lastIndex = indexFilename.lastIndexOf("/")
    val (_, last) = indexFilename.splitAt(lastIndex + 1)

    Try(Offset(last.toLong)).toEither
  }

}
