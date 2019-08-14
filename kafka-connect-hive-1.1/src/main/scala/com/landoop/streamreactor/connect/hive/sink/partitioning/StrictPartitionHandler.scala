package com.landoop.streamreactor.connect.hive.sink.partitioning

import com.landoop.streamreactor.connect.hive.{DatabaseName, Partition, TableName}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * A [[PartitionHandler]] that requires any partition
  * to already exist in the metastore.
  */
object StrictPartitionHandler extends PartitionHandler {

  override def path(partition: Partition,
                    db: DatabaseName,
                    tableName: TableName)
                   (client: IMetaStoreClient,
                    fs: FileSystem): Try[Path] = {
    try {
      val part = client.getPartition(db.value, tableName.value, partition.entries.map(_._2).toList.asJava)
      Success(new Path(part.getSd.getLocation))
    } catch {
      case NonFatal(e) =>
        Failure(new RuntimeException(s"Partition '${partition.entries.map(_._2).toList.mkString(",")}' does not exist and strict policy requires upfront creation", e))
    }
  }
}
