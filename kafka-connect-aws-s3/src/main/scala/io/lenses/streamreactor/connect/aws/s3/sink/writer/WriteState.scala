package io.lenses.streamreactor.connect.aws.s3.sink.writer

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import org.apache.kafka.connect.data.Schema

import java.io.File

sealed abstract class WriteState(commitState: CommitState) {
  def getCommitState = commitState
}

case class NoWriter(commitState: CommitState) extends WriteState(commitState) with LazyLogging {

  def toWriting(
    s3FormatWriter:    S3FormatWriter,
    file:              File,
    uncommittedOffset: Offset,
  ): Writing = {
    logger.debug("state transition: NoWriter => Writing")
    Writing(commitState, s3FormatWriter, file, uncommittedOffset)
  }

}

case class Writing(
  commitState:       CommitState,
  s3FormatWriter:    S3FormatWriter,
  file:              File,
  uncommittedOffset: Offset,
) extends WriteState(commitState)
    with LazyLogging {

  def updateOffset(o: Offset, schema: Option[Schema]): WriteState = {
    logger.debug(s"state update: Uncommitted offset update ${uncommittedOffset} => $o")
    copy(
      uncommittedOffset = o,
      commitState = commitState
        .offsetChange(
          schema,
          s3FormatWriter.getPointer,
        ),
    )
  }

  def toUploading(): Uploading = {
    logger.debug("state transition: Writing => Uploading")
    Uploading(commitState.reset(), file, uncommittedOffset)
  }
}

case class Uploading(
  commitState:       CommitState,
  file:              File,
  uncommittedOffset: Offset,
) extends WriteState(commitState)
    with LazyLogging {

  def toNoWriter(): NoWriter = {
    logger.debug("state transition: Uploading => NoWriter")
    NoWriter(commitState.withCommittedOffset(uncommittedOffset))
  }

}
