/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.TopicPartition
import com.landoop.streamreactor.connect.hive.TopicPartitionOffset
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
  * The [[StageManager]] handles creation of new files (staging) and
  * publishing of said files (committing).
  *
  * When a [[HiveWriter]] needs to write to a new file, it will invoke
  * the stage method with the partition or table location. A new file
  * will be generated using the [[TopicPartitionOffset]] information.
  *
  * After a file has been closed, commit will be called with the path
  * and the stage manager will make the file visible.
  */
class StageManager(filenamePolicy: FilenamePolicy) {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private def stageFilename(tp: TopicPartition) =
    s".${filenamePolicy.prefix}_${tp.topic.value}_${tp.partition}"

  private def finalFilename(tpo: TopicPartitionOffset) =
    s"${filenamePolicy.prefix}_${tpo.topic.value}_${tpo.partition}_${tpo.offset.value}"

  def stage(dir: Path, tp: TopicPartition)(implicit fs: FileSystem): Path = {
    val filename  = stageFilename(tp)
    val stagePath = new Path(dir, filename)
    fs.delete(stagePath, false)
    stagePath
  }

  def commit(stagePath: Path, tpo: TopicPartitionOffset)(implicit fs: FileSystem): Path = {
    val finalPath = new Path(stagePath.getParent, finalFilename(tpo))
    logger.info(s"Commiting file $stagePath=>$finalPath")
    fs.rename(stagePath, finalPath)
    finalPath
  }
}
