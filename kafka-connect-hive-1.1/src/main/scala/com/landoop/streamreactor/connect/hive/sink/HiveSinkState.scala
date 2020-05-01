package com.landoop.streamreactor.connect.hive.sink

import com.landoop.streamreactor.connect.hive
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.config.TableOptions
import com.landoop.streamreactor.connect.hive.sink.mapper.{DropPartitionValuesMapper, MetastoreSchemaAlignMapper, ProjectionMapper}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.kafka.connect.data.{Schema, Struct}

case class HiveSinkState(offsets: Map[TopicPartition, Offset],
                         committedOffsets: Map[TopicPartition, Offset],
                         table: Table,
                         tableLocation: Path,
                         plan: Option[PartitionPlan],
                         metastoreSchema: Schema,
                         mapper: Struct => Struct,
                         lastSchema: Schema) {
  def withTopicPartitionOffset(tpo: TopicPartitionOffset): HiveSinkState = {
    copy(offsets = offsets + (tpo.toTopicPartition -> tpo.offset))
  }

  def withTopicPartitionOffset(tp: TopicPartition, offset: Offset): HiveSinkState = {
    copy(offsets = offsets + (tp -> offset))
  }

  def withCommittedOffset(offsets: Map[TopicPartition, Offset]): HiveSinkState = {
    copy(committedOffsets = committedOffsets ++ offsets)
  }

  def withCommittedOffset(tp: TopicPartition, offset: Offset): HiveSinkState = {
    copy(committedOffsets = committedOffsets + (tp -> offset))
  }

  def withLastSchema(schema: Schema): HiveSinkState = copy(lastSchema = schema)
}

object HiveSinkState {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  def from(schema: Schema,
           table: TableOptions,
           dbName: DatabaseName)(implicit client: IMetaStoreClient, fs: FileSystem) = {
    logger.info(s"Init sink for schema $schema")

    val hiveTable = getOrCreateTable(table, dbName, schema)
    val tableLocation = new Path(hiveTable.getSd.getLocation)
    val plan = hive.partitionPlan(hiveTable)
    val metastoreSchema = table.evolutionPolicy
      .evolve(dbName, table.tableName, HiveSchemas.toKafka(hiveTable), schema)
      .getOrElse(sys.error(s"Unable to retrieve or evolve schema for $schema"))

    val mapperFns: Seq[Struct => Struct] = Seq(
      table.projection.map(new ProjectionMapper(_)),
      Some(new MetastoreSchemaAlignMapper(metastoreSchema)),
      plan.map(new DropPartitionValuesMapper(_))
    ).flatten.map(mapper => mapper.map _)

    val mapper = Function.chain(mapperFns)

    HiveSinkState(Map.empty, Map.empty, hiveTable, tableLocation, plan, metastoreSchema, mapper, schema)
  }

  def getOrCreateTable(table: TableOptions, dbName: DatabaseName, schema: Schema)
                      (implicit client: IMetaStoreClient, fs: FileSystem): Table = {

    def create: Table = {
      val partstring = if (table.partitions.isEmpty) "<no-partitions>" else table.partitions.mkString(",")
      logger.info(s"Creating table in hive [${dbName.value}.${table.tableName.value}, partitions=$partstring]")
      hive.createTable(dbName, table.tableName, schema, table.partitions, table.location, table.format)
    }

    logger.debug(s"Fetching or creating table ${dbName.value}.${table.tableName.value}")
    client.tableExists(dbName.value, table.tableName.value) match {
      case true if table.overwriteTable =>
        hive.dropTable(dbName, table.tableName, true)
        create
      case true => client.getTable(dbName.value, table.tableName.value)
      case false if table.createTable => create
      case false => throw new RuntimeException(s"Table ${dbName.value}.${table.tableName.value} does not exist")
    }
  }
}
