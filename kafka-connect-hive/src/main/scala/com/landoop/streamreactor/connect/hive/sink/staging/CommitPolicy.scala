package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.TopicPartitionOffset
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration.FiniteDuration

/**
 * The [[CommitPolicy]] is responsible for determining when
 * a file should be flushed (closed on disk, and moved to be visible).
 *
 * Typical implementations will flush based on number of records,
 * file size, or time since the file was opened.
 */
trait CommitPolicy {

  /**
   * This method is invoked after a file has been written.
   *
   * If the output file should be committed at this time, then this
   * method should return true, otherwise false.
   *
   * Once a commit has taken place, a new file will be opened
   * for the next record.
   *
   *
   */
  def shouldFlush(context: CommitContext)(implicit fs: FileSystem): Boolean
}

/**
 * @param tpo              the [[TopicPartitionOffset]] of the last record written
 * @param path             the path of the file that the struct was written to
 * @param count            the number of records written thus far to the file
 * @param createdTimestamp the time in milliseconds when the the file was created/accessed first time
 */
case class CommitContext(tpo: TopicPartitionOffset,
                         path: Path,
                         count: Long,
                         fileSize: Long,
                         createdTimestamp: Long)

/**
 * Default compile of [[CommitPolicy]] that will flush the
 * output file under the following circumstances:
 * - file size reaches limit
 * - time since file was created
 * - number of files is reached
 *
 * @param interval in millis
 */
case class DefaultCommitPolicy(fileSize: Option[Long],
                               interval: Option[FiniteDuration],
                               fileCount: Option[Long]) extends CommitPolicy {
  require(fileSize.isDefined || interval.isDefined || fileCount.isDefined)
  override def shouldFlush(context: CommitContext)
                          (implicit fs: FileSystem): Boolean = {
    val open_time = System.currentTimeMillis() - context.createdTimestamp
    fileSize.exists(_ <= context.fileSize) ||
      interval.exists(_.toMillis <= open_time) ||
      fileCount.exists(_ <= context.count)
  }
}