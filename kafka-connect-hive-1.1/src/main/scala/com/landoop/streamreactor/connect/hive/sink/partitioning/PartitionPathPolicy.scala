package com.landoop.streamreactor.connect.hive.sink.partitioning

import com.landoop.streamreactor.connect.hive.{Partition, PartitionKey}
import org.apache.hadoop.fs.Path

/**
  * Strategy interface used to determine where new partitions are written.
  */
trait PartitionPathPolicy {
  def path(tableLocation: Path, partition: Partition): Path
}

object PartitionPathPolicy {
  implicit val default = DefaultMetastorePartitionPathPolicy
}

/**
  * The default compile of PartitionLocationPolicy which uses
  * the same method as the metastore. That is new partitions will be written
  * under the table location in a key1=value1/key2=value2 format.
  */
object DefaultMetastorePartitionPathPolicy extends PartitionPathPolicy {
  override def path(tableLocation: Path, partition: Partition): Path = {
    partition.entries.foldLeft(tableLocation) { case (path, (PartitionKey(key), value)) =>
      new Path(path, s"$key=$value")
    }
  }
}
