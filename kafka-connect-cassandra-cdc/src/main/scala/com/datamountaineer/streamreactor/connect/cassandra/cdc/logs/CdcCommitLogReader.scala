/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.cassandra.cdc.logs

import java.io.File

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CdcConfig
import com.datamountaineer.streamreactor.connect.cassandra.cdc.logs.PartitionUpdateExtensions._
import com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata.SubscriptionDataProvider
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.cassandra.db.Mutation
import org.apache.cassandra.db.commitlog.CommitLogReadHandler.CommitLogReadException
import org.apache.cassandra.db.commitlog.CommitLogReplayer.CommitLogReplayException
import org.apache.cassandra.db.commitlog._
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConversions._


/**
  * Provides the implementation for [[CommitLogReadHandler]]. For each Cassandra mutation the
  * `handleMutation` is called. If the mutation is involving any of the the column families we are subscribed for CDC
  * a Change object is created.
  */
class CdcCommitLogReader(callback: SourceRecord => Unit)(implicit dataProvider: SubscriptionDataProvider, cdcConfig: CdcConfig) extends CommitLogReadHandler with StrictLogging {
  private val reader = new CommitLogReader()

  def read(file: File, position: CommitLogPosition, maxMutations: Int): Unit = {
    reader.readCommitLogSegment(this, file, position, maxMutations, true)
  }

  override def shouldSkipSegmentOnError(exception: CommitLogReadException): Boolean = {
    if (exception.permissible) {
      logger.error("Ignoring commit log replay error likely due to incomplete flush to disk", exception)
      // else if (Boolean.getBoolean(IGNORE_REPLAY_ERRORS_PROPERTY))
      //   logger.error("Ignoring commit log replay error", exception);
    }
    else if (!CommitLog.handleCommitError("Failed commit log replay", exception)) {
      logger.error("Replay stopped. If you wish to override this error and continue starting the node ignoring " +
        "commit log replay problems, specify -D IGNORE_REPLAY_ERRORS_PROPERTY =true " +
        "on the command line")
      throw new CommitLogReplayException(exception.getMessage, exception)
    }
    false
  }

  override def handleMutation(m: Mutation, size: Int, entryLocation: Int, desc: CommitLogDescriptor): Unit = {
    dataProvider.getColumnFamilies(m.getKeyspaceName).foreach { columnFamilySet =>
      m.getPartitionUpdates
        .filter(pu => columnFamilySet.contains(pu.metadata().cfName))
        .foreach { pu =>
          pu.read(m.getKeyspaceName, entryLocation, desc, callback)
        }
    }
  }

  override def handleUnrecoverableError(exception: CommitLogReadException): Unit = {
    logger.error(exception.toString)
  }

}
