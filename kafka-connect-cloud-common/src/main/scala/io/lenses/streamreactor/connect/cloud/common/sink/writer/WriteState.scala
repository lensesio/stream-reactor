/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
import org.apache.kafka.connect.data.Schema

import java.io.File

sealed abstract class WriteState(commitState: CommitState) {
  def getCommitState: CommitState = commitState
}

case class NoWriter(commitState: CommitState) extends WriteState(commitState) with LazyLogging {

  def toWriting(
    formatWriter:      FormatWriter,
    file:              File,
    uncommittedOffset: Offset,
    recordTimestamp:   Long,
  ): Writing = {
    logger.debug("state transition: NoWriter => Writing")
    Writing(commitState,
            formatWriter,
            file,
            firstBufferedOffset = uncommittedOffset,
            uncommittedOffset,
            recordTimestamp,
            recordTimestamp,
    )
  }

}

case class Writing(
  commitState:             CommitState,
  formatWriter:            FormatWriter,
  file:                    File,
  firstBufferedOffset:     Offset,
  uncommittedOffset:       Offset,
  earliestRecordTimestamp: Long,
  latestRecordTimestamp:   Long,
) extends WriteState(commitState)
    with LazyLogging {

  def update(o: Offset, recordTimestamp: Long, schema: Option[Schema]): WriteState = {
    logger.debug(
      s"state update: Uncommitted offset update $uncommittedOffset => $o, earliest record timestamp $earliestRecordTimestamp => $recordTimestamp",
    )
    copy(
      uncommittedOffset = o,
      commitState = commitState
        .offsetChange(
          schema,
          formatWriter.getPointer,
        ),
      earliestRecordTimestamp = math.min(earliestRecordTimestamp, recordTimestamp),
      latestRecordTimestamp   = math.max(latestRecordTimestamp, recordTimestamp),
    )
  }

  def toUploading: Uploading = {
    logger.debug("state transition: Writing => Uploading")
    Uploading(
      commitState.reset(),
      file,
      firstBufferedOffset,
      uncommittedOffset,
      earliestRecordTimestamp,
      latestRecordTimestamp,
      recordCount = commitState.recordCount,
    )
  }
}

case class Uploading(
  commitState:             CommitState,
  file:                    File,
  firstBufferedOffset:     Offset,
  uncommittedOffset:       Offset,
  earliestRecordTimestamp: Long,
  latestRecordTimestamp:   Long,
  recordCount:             Long,
) extends WriteState(commitState)
    with LazyLogging {

  def toNoWriter(newOffset: Option[Offset]): NoWriter = {
    logger.debug("state transition: Uploading => NoWriter")
    NoWriter(commitState.copy(committedOffset = newOffset.orElse(commitState.committedOffset)))
  }

  /**
   * Records the outcome of a successful `stage()`: the bytes are durable at `tempPath` with
   * `tempETag`, and `finalPath` is the destination the later `CopyOperation` must use. Computing
   * the object key once, here, is what makes the copy recorded in the master lock's `PendingState`
   * independent of anything the writer does afterwards.
   */
  def toStaged(bucket: String, tempPath: String, tempETag: String, finalPath: String): Staged = {
    logger.debug("state transition: Uploading => Staged")
    Staged(
      commitState,
      file,
      firstBufferedOffset,
      uncommittedOffset,
      earliestRecordTimestamp,
      latestRecordTimestamp,
      recordCount,
      bucket,
      tempPath,
      tempETag,
      finalPath,
    )
  }

}

/**
 * A writer whose bytes are already durable at a connector-scoped temp path and which is waiting
 * for the partition-batch master-lock CAS to move them to `finalPath`.
 *
 * Only reachable in `CommitMode.Batch`. The local staging file is deliberately retained until
 * `finalizeCommit`, so a failure anywhere before the CAS can be retried without re-formatting.
 */
case class Staged(
  commitState:             CommitState,
  file:                    File,
  firstBufferedOffset:     Offset,
  uncommittedOffset:       Offset,
  earliestRecordTimestamp: Long,
  latestRecordTimestamp:   Long,
  recordCount:             Long,
  bucket:                  String,
  tempPath:                String,
  tempETag:                String,
  finalPath:               String,
) extends WriteState(commitState)
    with LazyLogging {

  def copyOp: CopyOperation = CopyOperation(bucket, tempPath, finalPath, tempETag)

  // No deleteOp: every CopyOperation runs via `storageInterface.mvFile`, which MOVES the object
  // on all three backends (copy + delete source). By the time the commit chain returns `Right`,
  // every temp this writer staged is already gone -- a separate post-commit delete would be
  // dead code (see WriterCommitManager.commitBatch's doc).

  /**
   * The batch offset `newOffset` is the max `uncommittedOffset` across the batch, so it is normally
   * >= this writer's own committed offset. The `max` keeps the transition monotone even if a writer
   * carried a higher committed offset into the batch (for example after a rollback re-seed).
   */
  def toNoWriter(newOffset: Offset): NoWriter = {
    logger.debug("state transition: Staged => NoWriter")
    val committed = commitState.committedOffset.fold(newOffset)(existing =>
      if (existing.value >= newOffset.value) existing else newOffset,
    )
    NoWriter(commitState.copy(committedOffset = Some(committed)))
  }
}
