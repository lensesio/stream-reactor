package com.landoop.streamreactor.connect

import java.util

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive.formats.HiveFormat
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.api.{FieldSchema, SerDeInfo, StorageDescriptor, Table}
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType}
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

package object hive extends StrictLogging {

  /**
   * Returns all the partition keys from the given database and table.
   * A partition key is a field or column that has been designated as part of the partition
   * plan for this table.
   */
  def partitionPlan(db: DatabaseName, tableName: TableName)
                   (implicit client: IMetaStoreClient): Option[PartitionPlan] =
    partitionPlan(client.getTable(db.value, tableName.value))

  def partitionPlan(table: Table): Option[PartitionPlan] = {
    val keys = Option(table.getPartitionKeys).map(_.asScala).getOrElse(Nil).map { fs => PartitionKey(fs.getName) }
    if (keys.isEmpty) None else Some(PartitionPlan(TableName(table.getTableName), NonEmptyList.fromListUnsafe(keys.toList)))
  }

  def partitions(): Unit = {

  }

  def tableLocation(db: DatabaseName, tableName: TableName)
                   (implicit client: IMetaStoreClient): String =
    client.getTable(db.value, tableName.value).getSd.getLocation

  def serde(db: DatabaseName, tableName: TableName)
           (implicit client: IMetaStoreClient): Serde = {
    serde(client.getTable(db.value, tableName.value))
  }

  def serde(table: Table): Serde = {
    Serde(
      table.getSd.getSerdeInfo.getSerializationLib,
      table.getSd.getInputFormat,
      table.getSd.getOutputFormat,
      Option(table.getSd.getSerdeInfo.getParameters).fold(Map.empty[String, String])(_.asScala.toMap)
    )
  }

  def partitionKeys(db: DatabaseName, tableName: TableName)
                   (implicit client: IMetaStoreClient): Seq[PartitionKey] = {
    client.getTable(db.value, tableName.value).getPartitionKeys.asScala.map { key =>
      PartitionKey(key.getName)
    }.toList
  }

  def dropTable(db: DatabaseName, tableName: TableName, deleteData: Boolean)
               (implicit client: IMetaStoreClient, fs: FileSystem): Unit = {
    logger.info(s"Dropping table ${db.value}.${tableName.value}")
    val table = client.getTable(db.value, tableName.value)
    val locations = table.getSd.getLocation +:
      client.listPartitions(db.value, tableName.value, Short.MaxValue).asScala.map(_.getSd.getLocation)
    client.dropTable(db.value, tableName.value, deleteData, true)
    // if an external table then we need to manually remove the files
    if (table.getTableType == TableType.EXTERNAL_TABLE.name) {
      locations.map(new Path(_)).foreach(fs.delete(_, true))
    }
  }

  def createTable(db: DatabaseName,
                  tableName: TableName,
                  schema: Schema,
                  partitions: Seq[PartitionField],
                  location: Option[String],
                  format: HiveFormat)
                 (implicit client: IMetaStoreClient, fs: FileSystem): Table = {
    logger.info(s"Creating table with storedas=$format")

    val params = new util.HashMap[String, String]()
    params.put("CREATED_BY", getClass.getPackage.getName)

    val partitionKeys = partitions.map { field =>
      new FieldSchema(field.name, HiveSchemas.toHiveType(field.schema), field.comment.orNull)
    }

    val partitionKeyNames = partitionKeys.map(_.getName)

    // partition keys must not be included in the general columns
    val cols = HiveSchemas.toFieldSchemas(schema).filterNot(field => partitionKeyNames.contains(field.getName))

    val table = new Table()
    table.setDbName(db.value)
    table.setTableName(tableName.value)
    table.setOwner("hive")
    table.setCreateTime((System.currentTimeMillis / 1000).toInt)
    table.setParameters(params)
    table.setPartitionKeys(partitionKeys.asJava)

    location match {
      case Some(_) =>
        params.put("EXTERNAL", "TRUE")
        table.setTableType("EXTERNAL_TABLE")
      case _ =>
        table.setTableType("MANAGED_TABLE")
    }

    val dbloc = client.getDatabase(db.value).getLocationUri
    require(dbloc.trim.nonEmpty)
    val defaultLocation = s"$dbloc/${tableName.value}"

    val sd = new StorageDescriptor()
    sd.setLocation(location.getOrElse(defaultLocation))
    sd.setInputFormat(format.serde.inputFormat)
    sd.setOutputFormat(format.serde.outputFormat)
    sd.setSerdeInfo(new SerDeInfo(null, format.serde.serializationLib, format.serde.params.asJava))
    sd.setCols(cols.asJava)

    table.setSd(sd)

    client.createTable(table)

    try {
      fs.mkdirs(new Path(table.getSd.getLocation))
    } catch {
      case NonFatal(e) => logger.error(s"Error creating table directory at ${table.getSd.getLocation}", e)
    }

    table
  }

  /**
   * Returns the partitions for a given table.
   * This may be an empty seq, if the table has partition keys defined but no data yet written
   */
  def partitions(db: DatabaseName, tableName: TableName)
                (implicit client: IMetaStoreClient): Seq[Partition] = {
    partitionKeys(db, tableName) match {
      case Nil => Nil
      case keys => partitions(db, tableName, PartitionPlan(tableName, NonEmptyList.fromListUnsafe(keys.toList)))
    }
  }

  def schema(db: DatabaseName, tableName: TableName)
            (implicit client: IMetaStoreClient): Schema = {
    val table = client.getTable(db.value, tableName.value)
    HiveSchemas.toKafka(table)
  }

  /**
   * Returns the partitions for a given table.
   * This may be an empty seq, if the table has partition keys defined but no data yet written
   */
  def partitions(db: DatabaseName, tableName: TableName, plan: PartitionPlan)
                (implicit client: IMetaStoreClient): Seq[Partition] = {
    // for each partition we take the values and associate with the partition keys
    client.listPartitions(db.value, tableName.value, Short.MaxValue).asScala.map { p =>
      val values = NonEmptyList.fromListUnsafe(p.getValues.asScala.toList)
      require(values.size == plan.keys.size, "A partition value must be defined for each partition key")
      val entries = plan.keys.zipWith(values)((a, b) => (a, b))
      Partition(entries, Some(new Path(p.getSd.getLocation)))
    }
  }

  // returns a partition generated from the input struct
  // each struct must supply a non null value for each partition key
  def partition(struct: Struct, plan: PartitionPlan): Partition = {
    val entries = plan.keys.map { key =>
      Option(struct.get(key.value)) match {
        case None => sys.error(s"Partition value for $key must be defined")
        case Some(null) => sys.error(s"Partition values cannot be null [was null for $key]")
        case Some(value) => key -> value.toString
      }
    }
    Partition(entries, None)
  }
}