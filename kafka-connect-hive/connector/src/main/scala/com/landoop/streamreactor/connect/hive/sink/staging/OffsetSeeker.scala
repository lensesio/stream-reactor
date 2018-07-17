package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * The [[OffsetSeeker]] is responsible for querying the [[FileSystem]] to
  * retrieve current offset information from a table.
  *
  * @param filenamePolicy we need the policy so we can match on this.
  */
class OffsetSeeker(filenamePolicy: FilenamePolicy) {

  import HdfsUtils._

  def seek(db: DatabaseName, tableName: TableName)
          (implicit fs: FileSystem, client: IMetaStoreClient): Set[TopicPartitionOffset] = {
    val loc = com.landoop.streamreactor.connect.hive.tableLocation(db, tableName)
    val prefix = filenamePolicy.prefix
    fs.ls(new Path(loc), true).map(_.getPath.getName).collect {
      case CommittedFileName(`prefix`, topic, partition, _, end) => TopicPartitionOffset(topic, partition, end)
    }.toSeq.groupBy(_.toTopicPartition).map { case (tp, tpo) =>
      tp.withOffset(tpo.maxBy(_.offset.value).offset)
    }.toSet
  }
}