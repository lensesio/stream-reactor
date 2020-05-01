package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.formats.HiveWriter
import com.landoop.streamreactor.connect.hive.{TopicPartition, TopicPartitionOffset}
import org.apache.hadoop.fs.{FileSystem, Path}

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
    val filename = stageFilename(tp)
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