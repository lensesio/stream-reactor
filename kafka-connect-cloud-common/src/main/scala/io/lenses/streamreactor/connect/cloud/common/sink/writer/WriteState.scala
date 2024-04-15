/*
 * Copyright 2017-2024 Lenses.io Ltd
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
    Writing(commitState, formatWriter, file, uncommittedOffset, recordTimestamp)
  }

}

case class Writing(
  commitState:             CommitState,
  formatWriter:            FormatWriter,
  file:                    File,
  uncommittedOffset:       Offset,
  earliestRecordTimestamp: Long,
) extends WriteState(commitState)
    with LazyLogging {

  //TODO: it's not clear why we are only keeping track of one schema (VALUE) and not key/and headers
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
    )
  }

  def toUploading: Uploading = {
    logger.debug("state transition: Writing => Uploading")
    Uploading(commitState.reset(), file, uncommittedOffset, earliestRecordTimestamp)
  }
}

case class Uploading(
  commitState:             CommitState,
  file:                    File,
  uncommittedOffset:       Offset,
  earliestRecordTimestamp: Long,
) extends WriteState(commitState)
    with LazyLogging {

  def toNoWriter: NoWriter = {
    logger.debug("state transition: Uploading => NoWriter")
    NoWriter(commitState.withCommittedOffset(uncommittedOffset))
  }

}
