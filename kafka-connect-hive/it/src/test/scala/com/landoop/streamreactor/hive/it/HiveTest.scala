package com.landoop.streamreactor.connect.hive.it

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.formats.ParquetHiveFormat
import org.apache.hadoop.hive.metastore.api.{FieldSchema, SerDeInfo, StorageDescriptor, Table}
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, TableType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

class HiveTest extends AnyFlatSpec with Matchers with HiveTestConfig {

  // TODO: Refactor me!
  def setUpData()(implicit client: HiveMetaStoreClient): Unit = {
    {
      val table = new Table()
      table.setDbName("default")
      table.setTableName("non_partitioned_table")
      table.setOwner("hive")
      table.setTableType(TableType.EXTERNAL_TABLE.name())
      table.setCreateTime((System.currentTimeMillis / 1000).toInt)
      table.setRetention(0)
      table.setParameters(new util.HashMap())

      val cols = Seq(
        new FieldSchema("a", "string", "lovely field"),
        new FieldSchema("b", "int", null),
        new FieldSchema("c", "boolean", null)
      )

      val sd = new StorageDescriptor()
      sd.setCompressed(false)
      sd.setLocation(client.getDatabase("default").getLocationUri + "/non_partitioned_table")
      sd.setInputFormat(ParquetHiveFormat.serde.inputFormat)
      sd.setOutputFormat(ParquetHiveFormat.serde.outputFormat)
      sd.setSerdeInfo(new SerDeInfo(null, ParquetHiveFormat.serde.serializationLib, ParquetHiveFormat.serde.params.asJava))
      sd.setCols(cols.asJava)
      table.setSd(sd)

      client.createTable(table)
    }

    {

      val partkeys = Seq(
        new FieldSchema("x", "string", "my first partition key"),
        new FieldSchema("y", "int", null)
      )

      val table = new Table()
      table.setDbName("default")
      table.setTableName("partitioned_table")
      table.setOwner("hive")
      table.setTableType(TableType.EXTERNAL_TABLE.name())
      table.setCreateTime((System.currentTimeMillis / 1000).toInt)
      table.setRetention(0)
      table.setParameters(new util.HashMap())
      table.setPartitionKeys(partkeys.asJava)

      val cols = Seq(
        new FieldSchema("a", "string", "lovely field"),
        new FieldSchema("b", "int", null),
        new FieldSchema("c", "boolean", null)
      )

      val sd = new StorageDescriptor()
      sd.setCompressed(false)
      sd.setLocation(client.getDatabase("default").getLocationUri + "/partitioned_table")
      sd.setInputFormat(ParquetHiveFormat.serde.inputFormat)
      sd.setOutputFormat(ParquetHiveFormat.serde.outputFormat)
      sd.setSerdeInfo(new SerDeInfo(null, ParquetHiveFormat.serde.serializationLib, ParquetHiveFormat.serde.params.asJava))
      sd.setCols(cols.asJava)
      table.setSd(sd)

      client.createTable(table)
    }
  }

  "hive" should "read None for partition plan of non-partitioned table" in {
    implicit val (client, _) = testInit("default")
    setUpData()
    partitionPlan(DatabaseName("default"), TableName("non_partitioned_table")) shouldBe None
  }

  it should "read partition plan for partitioned table" in {
    implicit val (client, _) = testInit("default")
    partitionPlan(DatabaseName("default"), TableName("partitioned_table")).get shouldBe
      PartitionPlan(TableName("partitioned_table"), NonEmptyList.of(PartitionKey("x"), PartitionKey("y")))
  }

  it should "read table location" in {
    implicit val (client, _) = testInit("default")
    tableLocation(DatabaseName("default"), TableName("non_partitioned_table")) shouldBe "hdfs://namenode:8020/user/hive/warehouse/non_partitioned_table"
  }

  it should "read serde info" in {
    implicit val (client, _) = testInit("default")
    serde(DatabaseName("default"), TableName("non_partitioned_table")) shouldBe ParquetHiveFormat.serde
  }
}
