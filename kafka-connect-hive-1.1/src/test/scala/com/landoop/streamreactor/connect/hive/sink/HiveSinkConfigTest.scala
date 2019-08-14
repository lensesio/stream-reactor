package com.landoop.streamreactor.connect.hive.sink

import com.landoop.streamreactor.connect.hive.PartitionField
import com.landoop.streamreactor.connect.hive.TableName
import com.landoop.streamreactor.connect.hive.Topic
import com.landoop.streamreactor.connect.hive.formats.ParquetHiveFormat
import com.landoop.streamreactor.connect.hive.sink.config.HiveSinkConfig
import com.landoop.streamreactor.connect.hive.sink.evolution.AddEvolutionPolicy
import com.landoop.streamreactor.connect.hive.sink.partitioning.DynamicPartitionHandler
import com.landoop.streamreactor.connect.hive.sink.staging.DefaultCommitPolicy
import com.landoop.streamreactor.connect.hive.TopicPartition
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.Matchers
import org.scalatest.WordSpec

class HiveSinkConfigTest extends WordSpec with Matchers {

  "HiveSink" should {
    "populate required table properties from KCQL" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.topic shouldBe Topic("mytopic")
      tableConfig.tableName shouldBe TableName("mytable")
      tableConfig.projection.get.map(_.getName).toList shouldBe Seq("a", "b", "c")
    }
    "populate aliases from KCQL" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.projection.get.map(_.getName).toList shouldBe Seq("a", "b", "c")
      tableConfig.projection.get.map(_.getAlias).toList shouldBe Seq("x", "b", "z")
    }
    "set projection to None for *" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select * from mytopic"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.projection shouldBe None
    }
    "default to parquet when format is not specified" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.format shouldBe ParquetHiveFormat
    }
    "populate schema evolution policy from KCQL" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic with_schema_evolution=add"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.evolutionPolicy shouldBe AddEvolutionPolicy
    }
    "populate commitPolicy from KCQL" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic WITH_FLUSH_SIZE=912 WITH_FLUSH_INTERVAL=1 WITH_FLUSH_COUNT=333"
      ))
      val tableConfig = config.tableOptions.head
      import scala.concurrent.duration._
      tableConfig.commitPolicy shouldBe DefaultCommitPolicy(Option(912), Option(1.second), Option(333))
    }
    "populate partitions from KCQL" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic PARTITIONBY g,h,t"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.partitions shouldBe Seq(PartitionField("g"), PartitionField("h"), PartitionField("t"))
    }
    "populate partitioningPolicy from KCQL" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic WITH_PARTITIONING=dynamic"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.partitioner.getClass shouldBe classOf[DynamicPartitionHandler]
    }
    "populate create from KCQL" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic AUTOCREATE"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.createTable shouldBe true
    }
    "default to createTable=false" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.createTable shouldBe false
    }
    "populate overwrite from KCQL" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic WITH_OVERWRITE"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.overwriteTable shouldBe true
    }
    "default to overwriteTable=false" in {
      val config = HiveSinkConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a as x,b,c as z from mytopic"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.overwriteTable shouldBe false
    }

  }
}
