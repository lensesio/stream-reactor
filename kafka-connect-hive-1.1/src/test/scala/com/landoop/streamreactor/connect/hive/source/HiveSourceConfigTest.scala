package com.landoop.streamreactor.connect.hive.source

import com.landoop.streamreactor.connect.hive.source.config.{HiveSourceConfig, ProjectionField}
import com.landoop.streamreactor.connect.hive.{TableName, Topic}
import org.scalatest.{Matchers, WordSpec}

class HiveSourceConfigTest extends WordSpec with Matchers {

  "HiveSource" should {
    "populate required table properties from KCQL" in {
      val config = HiveSourceConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytopic select a,b from mytable"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.topic shouldBe Topic("mytopic")
      tableConfig.tableName shouldBe TableName("mytable")
      tableConfig.projection.get.toList shouldBe Seq(ProjectionField("a", "a"), ProjectionField("b", "b"))
    }
    "populate aliases from KCQL" in {
      val config = HiveSourceConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytopic select a as x,b from mytable"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.projection.get.toList shouldBe Seq(ProjectionField("a", "x"), ProjectionField("b", "b"))
    }
    "set projection to None for *" in {
      val config = HiveSourceConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytopic select * from mytable"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.projection shouldBe None
    }
    "set table limit" in {
      val config = HiveSourceConfig.fromProps(Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.metastore" -> "thrift",
        "connect.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytopic select a from mytable limit 200"
      ))
      val tableConfig = config.tableOptions.head
      tableConfig.limit shouldBe 200
    }
  }
}
