package com.landoop.streamreactor.connect.hive.sink.partitioning

import com.landoop.streamreactor.connect.hive.{DatabaseName, Partition, TableName}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{StorageDescriptor, Table}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * A [[PartitionHandler]] that creates partitions
  * on the fly as required.
  *
  * The path of the partition is determined by the given
  * [[PartitionPathPolicy]] parameter. By default this will
  * be an compile that uses the standard hive
  * paths of key1=value1/key2=value2.
  */
class DynamicPartitionHandler(pathPolicy: PartitionPathPolicy = DefaultMetastorePartitionPathPolicy)
  extends PartitionHandler {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def path(partition: Partition,
                    db: DatabaseName,
                    tableName: TableName)
                   (client: IMetaStoreClient,
                    fs: FileSystem): Try[Path] = {

    def table: Table = client.getTable(db.value, tableName.value)

    def create(path: Path, table: Table): Unit = {
      logger.debug(s"New partition will be created at $path")

      val sd = new StorageDescriptor(table.getSd)
      sd.setLocation(path.toString)

      val params = new java.util.HashMap[String, String]
      val values = partition.entries.map(_._2).toList.asJava
      val ts = (System.currentTimeMillis / 1000).toInt

      val p = new org.apache.hadoop.hive.metastore.api.Partition(values, db.value, tableName.value, ts, 0, sd, params)
      logger.debug(s"Updating hive metastore with partition $p")
      client.add_partition(p)

      logger.info(s"Partition has been created in metastore [$partition]")
    }

    Try(client.getPartition(db.value, tableName.value, partition.entries.toList.map(_._2).asJava)) match {
      case Success(p) => Try {
        new Path(p.getSd.getLocation)
      }
      case Failure(_) => Try {
        val t = table
        val tableLocation = new Path(t.getSd.getLocation)
        val path = pathPolicy.path(tableLocation, partition)
        create(path, t)
        path
      }
    }
  }
}
