package com.landoop.streamreactor.connect.hive.source

import java.util

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.HiveSink
import com.landoop.streamreactor.connect.hive.sink.config.{HiveSinkConfig, TableOptions}
import com.landoop.streamreactor.connect.hive.source.config.{HiveSourceConfig, ProjectionField, SourceTableOptions}
import com.landoop.streamreactor.connect.hive.source.offset.{HiveSourceOffsetStorageReader, MockOffsetStorageReader}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._
import scala.util.Try

class HiveSourceTest extends AnyWordSpec with Matchers with HiveTestConfig with StrictLogging {

  val dbname = "source_test2"

  Try {
    client.dropDatabase(dbname, true, true)
  }

  Try {
    client.dropDatabase(dbname)
  }

  Try {
    client.createDatabase(
      new Database(
        dbname,
        null,
        s"/user/hive/warehouse/$dbname",
        new util.HashMap()
      )
    )
  }

  val schema = SchemaBuilder
    .struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", SchemaBuilder.float64().optional().build())
    .build()

  def populateEmployees(table: String,
                        partitions: Seq[PartitionField] = Nil): Unit = {
    Try {
      client.dropTable(dbname, table, true, true)
    }
    val users = List(
      new Struct(schema)
        .put("name", "sam")
        .put("title", "mr")
        .put("salary", 100.43),
      new Struct(schema)
        .put("name", "laura")
        .put("title", "ms")
        .put("salary", 429.06),
      new Struct(schema)
        .put("name", "stef")
        .put("title", "ms")
        .put("salary", 329.06),
      new Struct(schema)
        .put("name", "andrew")
        .put("title", "mr")
        .put("salary", 529.06),
      new Struct(schema)
        .put("name", "ant")
        .put("title", "mr")
        .put("salary", 629.06),
      new Struct(schema)
        .put("name", "tom")
        .put("title", "miss")
        .put("salary", 395.44)
    )

    val sinkConfig = HiveSinkConfig(
      DatabaseName(dbname),
      tableOptions = Set(
        TableOptions(
          TableName(table),
          Topic("mytopic"),
          true,
          true,
          partitions = partitions
        )
      ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    val sink = HiveSink.from(TableName(table), sinkConfig)
    users.zipWithIndex.foreach {
      case (user, k) =>
        sink.write(user, TopicPartitionOffset(Topic("mytopic"), 1, Offset(k)))
    }
    sink.close()
  }

  "hive source" should {

    "read from a non partitioned table" in {

      val table = "employees"
      populateEmployees(table)

      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map.empty)
      )
      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions =
          Set(SourceTableOptions(TableName(table), Topic("mytopic"))),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )
      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      source.toList
        .map(_.value.asInstanceOf[Struct])
        .map(StructUtils.extractValues) shouldBe
        List(
          Vector("sam", "mr", 100.43),
          Vector("laura", "ms", 429.06),
          Vector("stef", "ms", 329.06),
          Vector("andrew", "mr", 529.06),
          Vector("ant", "mr", 629.06),
          Vector("tom", "miss", 395.44)
        )
    }

    "apply a projection to the read of a non-partitioned table" in {

      val table = "employees"
      populateEmployees(table)

      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map.empty)
      )
      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions = Set(
          SourceTableOptions(
            TableName(table),
            Topic("mytopic"),
            projection = Some(
              NonEmptyList.of(
                ProjectionField("title", "title"),
                ProjectionField("name", "name")
              )
            )
          )
        ),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )
      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      val records = source.toList
      records.head.valueSchema.fields().asScala.map(_.name) shouldBe Seq(
        "title",
        "name"
      )
      records
        .map(_.value.asInstanceOf[Struct])
        .map(StructUtils.extractValues)
        .toSet shouldBe
        Set(
          Vector("mr", "ant"),
          Vector("miss", "tom"),
          Vector("ms", "stef"),
          Vector("ms", "laura"),
          Vector("mr", "andrew"),
          Vector("mr", "sam")
        )
    }

    "apply a projection with aliases to the read of a non-partitioned table" in {

      val table = "employees"
      populateEmployees(table)

      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map.empty)
      )
      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions = Set(
          SourceTableOptions(
            TableName(table),
            Topic("mytopic"),
            projection = Some(
              NonEmptyList.of(
                ProjectionField("title", "salutation"),
                ProjectionField("name", "name")
              )
            )
          )
        ),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )
      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      val records = source.toList
      records.head.valueSchema.fields().asScala.map(_.name) shouldBe Seq(
        "salutation",
        "name"
      )
      records
        .map(_.value.asInstanceOf[Struct])
        .map(StructUtils.extractValues)
        .toSet shouldBe
        Set(
          Vector("mr", "ant"),
          Vector("miss", "tom"),
          Vector("ms", "stef"),
          Vector("ms", "laura"),
          Vector("mr", "andrew"),
          Vector("mr", "sam")
        )
    }

    "read from a partitioned table" in {

      val table = "employees_partitioned"
      populateEmployees(table, partitions = Seq(PartitionField("title")))

      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map.empty)
      )
      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions =
          Set(SourceTableOptions(TableName(table), Topic("mytopic"))),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )
      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      source
        .map(_.value.asInstanceOf[Struct])
        .map(StructUtils.extractValues)
        .toSet shouldBe
        Set(
          Vector("tom", 395.44, "miss"),
          Vector("sam", 100.43, "mr"),
          Vector("andrew", 529.06, "mr"),
          Vector("ant", 629.06, "mr"),
          Vector("laura", 429.06, "ms"),
          Vector("stef", 329.06, "ms")
        )
    }

    "apply a projection to the read of a partitioned table" in {

      val table = "employees_partitioned"
      populateEmployees(table, partitions = Seq(PartitionField("title")))

      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map.empty)
      )
      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions = Set(
          SourceTableOptions(
            TableName(table),
            Topic("mytopic"),
            projection = Some(
              NonEmptyList.of(
                ProjectionField("title", "title"),
                ProjectionField("name", "name")
              )
            )
          )
        ),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )
      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      source
        .map(_.value.asInstanceOf[Struct])
        .map(StructUtils.extractValues)
        .toSet shouldBe
        Set(
          Vector("mr", "ant"),
          Vector("miss", "tom"),
          Vector("ms", "stef"),
          Vector("ms", "laura"),
          Vector("mr", "andrew"),
          Vector("mr", "sam")
        )
    }

    "adhere to a LIMIT" in {
      val table = "employees"
      populateEmployees(table)

      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map.empty)
      )
      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions = Set(
          SourceTableOptions(TableName(table), Topic("mytopic"), limit = 3)
        ),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )

      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      source.toList
        .map(_.value.asInstanceOf[Struct])
        .map(StructUtils.extractValues) shouldBe
        List(
          Vector("sam", "mr", 100.43),
          Vector("laura", "ms", 429.06),
          Vector("stef", "ms", 329.06)
        )
    }

    "set offset and partition on each record" in {
      val table = "employees"
      populateEmployees(table)

      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map.empty)
      )
      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions =
          Set(SourceTableOptions(TableName(table), Topic("mytopic"))),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )

      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      val list = source.toList
      list.head.sourcePartition() shouldBe
        Map(
          "db" -> dbname,
          "table" -> table,
          "topic" -> "mytopic",
          "path" -> s"hdfs://localhost:8020/user/hive/warehouse/$dbname/$table/streamreactor_mytopic_1_5"
        ).asJava

      list.head.sourceOffset() shouldBe
        Map("rownum" -> "0").asJava

      list.last.sourcePartition() shouldBe
        Map(
          "db" -> dbname,
          "table" -> table,
          "topic" -> "mytopic",
          "path" -> s"hdfs://localhost:8020/user/hive/warehouse/$dbname/$table/streamreactor_mytopic_1_5"
        ).asJava

      list.last.sourceOffset() shouldBe
        Map("rownum" -> "5").asJava
    }

    "skip records based on offset storage" in {
      val table = "employees3"
      populateEmployees(table)

      val sourcePartition = SourcePartition(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        new Path(
          s"hdfs://localhost:8020/user/hive/warehouse/$dbname/$table/streamreactor_mytopic_1_5"
        )
      )
      val sourceOffset = SourceOffset(2)
      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map(sourcePartition -> sourceOffset))
      )

      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions =
          Set(SourceTableOptions(TableName(table), Topic("mytopic"))),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )
      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      source.toList
        .map(_.value.asInstanceOf[Struct])
        .map(StructUtils.extractValues) shouldBe
        List(
          Vector("stef", "ms", 329.06),
          Vector("andrew", "mr", 529.06),
          Vector("ant", "mr", 629.06),
          Vector("tom", "miss", 395.44)
        )
    }

    "skip records based on offset storage before applying limit" in {
      val table = "employees3"
      populateEmployees(table)

      val sourcePartition = SourcePartition(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        new Path(
          s"hdfs://localhost:8020/user/hive/warehouse/$dbname/$table/streamreactor_mytopic_1_5"
        )
      )
      val sourceOffset = SourceOffset(2)
      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map(sourcePartition -> sourceOffset))
      )

      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions = Set(
          SourceTableOptions(TableName(table), Topic("mytopic"), limit = 2)
        ),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )
      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      source.toList
        .map(_.value.asInstanceOf[Struct])
        .map(StructUtils.extractValues) shouldBe
        List(Vector("stef", "ms", 329.06), Vector("andrew", "mr", 529.06))
    }

    "skip file if offset is beyond file size" in {
      val table = "employees3"
      populateEmployees(table)

      val sourcePartition = SourcePartition(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        new Path(
          s"hdfs://localhost:8020/user/hive/warehouse/$dbname/$table/streamreactor_mytopic_1_5"
        )
      )
      val sourceOffset = SourceOffset(44)
      val reader = new HiveSourceOffsetStorageReader(
        new MockOffsetStorageReader(Map(sourcePartition -> sourceOffset))
      )

      val sourceConfig = HiveSourceConfig(
        DatabaseName(dbname),
        tableOptions = Set(
          SourceTableOptions(TableName(table), Topic("mytopic"), limit = 2)
        ),
        kerberos = None,
        hadoopConfiguration = HadoopConfiguration.Empty
      )
      val source = new HiveSource(
        DatabaseName(dbname),
        TableName(table),
        Topic("mytopic"),
        reader,
        sourceConfig
      )
      source.toList.isEmpty shouldBe true
    }
  }
}
