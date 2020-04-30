package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.util.control.NonFatal

/**
 * The [[OffsetSeeker]] is responsible for querying the [[FileSystem]] to
 * retrieve current offset information from a table.
 *
 * @param filenamePolicy we need the policy so we can match on this.
 */
class OffsetSeeker(filenamePolicy: FilenamePolicy) {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  import HdfsUtils._

  def seek(db: DatabaseName,
           tableName: TableName)
          (implicit fs: FileSystem, client: IMetaStoreClient): Set[TopicPartitionOffset] = {

    try {
      // the table may not have been created, in which case we have no offsets defined
      if (client.tableExists(db.value, tableName.value)) {
        val loc = com.landoop.streamreactor.connect.hive.tableLocation(db, tableName)
        val prefix = filenamePolicy.prefix

        fs.ls(new Path(loc), true)
          .map(_.getPath.getName)
          .collect {
            case CommittedFileName(`prefix`, topic, partition, _, end) => TopicPartitionOffset(topic, partition, end)
          }
          .toSeq
          .groupBy(_.toTopicPartition)
          .map { case (tp, tpo) =>
            tp.withOffset(tpo.maxBy(_.offset.value).offset)
          }.toSet

      } else {
        Set.empty
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error seeking table $db.table")
        throw e
    }
  }
}