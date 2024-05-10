package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.catsSyntaxEitherId
import org.apache.kafka.connect.source.SourceRecord

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.ListHasAsScala

object SourceRecordsLoop {

  def loop(task: S3SourceTask, timeout: Long, expectedSize: Int): Either[Throwable, Seq[SourceRecord]] = {
    @tailrec
    def loopUntilTimeout(
      records:     Seq[SourceRecord],
      endTime:     Long,
      currentTime: Long,
    ): Either[Throwable, Seq[SourceRecord]] =
      if (records.size == expectedSize) {
        records.asRight
      } else if (currentTime >= endTime) {
        new RuntimeException(s"Timeout of $timeout ms reached while polling for records").asLeft
      } else {
        val polledRecords  = task.poll().asScala
        val updatedRecords = records ++ polledRecords
        loopUntilTimeout(updatedRecords, endTime, System.currentTimeMillis())
      }

    loopUntilTimeout(Seq.empty, System.currentTimeMillis() + timeout, 0L)

  }
}
