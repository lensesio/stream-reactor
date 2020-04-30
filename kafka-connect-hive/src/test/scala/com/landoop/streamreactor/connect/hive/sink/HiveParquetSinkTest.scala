package com.landoop.streamreactor.connect.hive.sink

import java.util

import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.config.{HiveSinkConfig, TableOptions}
import com.landoop.streamreactor.connect.hive.sink.evolution.AddEvolutionPolicy
import com.landoop.streamreactor.connect.hive.sink.partitioning.StrictPartitionHandler
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.util.Try

class HiveParquetSinkTest extends AnyFlatSpec with Matchers with HiveTestConfig {

  val schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", SchemaBuilder.float64().optional().build())
    .build()

  val dbname = "sink_test"

  Try {
    client.dropDatabase(dbname)
  }

  Try {
    client.createDatabase(new Database(dbname, null, s"/user/hive/warehouse/$dbname", new util.HashMap()))
  }

  "hive sink" should "write to a non partitioned table" in {

    val users = List(
      new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06),
      new Struct(schema).put("name", "tom").put("title", null).put("salary", 395.44)
    )

    Try {
      client.dropTable(dbname, "employees", true, true)
    }

    val config = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName("employees"), Topic("mytopic"), true, true)
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    val sink = HiveSink.from(TableName("employees"), config)
    users.foreach(sink.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(1))))
    sink.close()
  }

  it should "write to a partitioned table" in {

    val table = "employees_partitioned"

    Try {
      client.dropTable(dbname, table, true, true)
    }

    val users = List(
      new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06)
    )

    val config = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName(table), Topic("mytopic"), true, true, partitions = Seq(PartitionField("title")))
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    val sink = HiveSink.from(TableName(table), config)
    users.foreach(sink.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(1))))
    sink.close()
  }

  it should "create new partitions in the metastore when using dynamic partitions" in {

    val table = "employees_dynamic_partitions"

    val users = List(
      new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06)
    )

    Try {
      client.dropTable(dbname, table, true, true)
    }

    val config = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName(table), Topic("mytopic"), true, true, partitions = Seq(PartitionField("title")))
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    val sink = HiveSink.from(TableName(table), config)
    users.foreach(sink.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(1))))
    sink.close()

    val partitions = client.listPartitions(dbname, table, Short.MaxValue).asScala

    partitions.exists { partition =>
      partition.getValues.asScala.toList == List("mr")
    } shouldBe true

    partitions.exists { partition =>
      partition.getValues.asScala.toList == List("ms")
    } shouldBe true

    partitions.exists { partition =>
      partition.getValues.asScala.toList == List("other")
    } shouldBe false
  }

  it should "allow setting table type of new tables" in {

    val users = List(new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43))

    val config1 = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName("abc"), Topic("mytopic"), true, true, partitions = Seq(PartitionField("title")))
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    Try {
      client.dropTable(dbname, "abc", true, true)
    }

    val sink1 = HiveSink.from(TableName("abc"), config1)
    users.foreach(sink1.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(1))))
    sink1.close()

    client.getTable(dbname, "abc").getTableType shouldBe "MANAGED_TABLE"

    val config2 = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName("abc"), Topic("mytopic"), true, true, location = Option("hdfs://localhost:8020/user/hive/warehouse/foo"))
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    Try {
      client.dropTable(dbname, "abc", true, true)
    }

    val sink2 = HiveSink.from(TableName("abc"), config2)
    users.foreach(sink2.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(1))))
    sink2.close()

    client.getTable(dbname, "abc").getTableType shouldBe "EXTERNAL_TABLE"
  }

  it should "use staging file per (topic,partition)" in {

    val user1 = new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43)
    val user2 = new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 417.61)

    val tableName = "stage_per_partition"

    Try {
      client.dropTable(dbname, tableName, true, true)
    }

    val config = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName(tableName), Topic("mytopic"), true, true)
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    val sink = HiveSink.from(TableName(tableName), config)
    sink.write(user1, TopicPartitionOffset(Topic("mytopic3"), 1, Offset(44)))
    sink.write(user2, TopicPartitionOffset(Topic("mytopic4"), 4, Offset(45)))

    fs.exists(new Path(s"/user/hive/warehouse/$dbname/$tableName/.streamreactor_mytopic3_1")) shouldBe true
    fs.exists(new Path(s"/user/hive/warehouse/$dbname/$tableName/.streamreactor_mytopic4_4")) shouldBe true

    sink.close()
  }

  it should "commit files when sink is closed" in {

    val user1 = new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43)
    val user2 = new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 417.61)

    val tableName = "commit_test"

    Try {
      client.dropTable(dbname, tableName, true, true)
    }

    val config = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName(tableName), Topic("mytopic"), true, true)
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    val sink = HiveSink.from(TableName(tableName), config)
    sink.write(user1, TopicPartitionOffset(Topic("mytopic"), 1, Offset(44)))
    fs.exists(new Path(s"/user/hive/warehouse/$dbname/$tableName/.streamreactor_mytopic_1")) shouldBe true

    sink.write(user2, TopicPartitionOffset(Topic("mytopic"), 1, Offset(45)))
    sink.close()

    fs.exists(new Path(s"/user/hive/warehouse/$dbname/$tableName/.streamreactor_mytopic_1")) shouldBe false
    fs.exists(new Path(s"/user/hive/warehouse/$dbname/$tableName/streamreactor_mytopic_1_45")) shouldBe true
  }

  it should "set partition keys in the sd column descriptors" in {

    val users = List(new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43))
    val tableName = "partition_keys_test"
    Try {
      client.dropTable(dbname, tableName, true, true)
    }

    val config = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName(tableName), Topic("mytopic"), true, true, partitions = Seq(PartitionField("title")))
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    val sink = HiveSink.from(TableName(tableName), config)
    users.foreach(sink.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(1))))
    sink.close()

    val table = client.getTable(dbname, tableName)
    table.getPartitionKeys.asScala.map(_.getName) shouldBe Seq("title")
    table.getSd.getCols.asScala.map(_.getName) shouldBe Seq("name", "salary")
  }

  it should "throw an exception if a partition doesn't exist with strict partitioning" in {

    val users = List(new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43))

    val tableName = "strict_partitioning_test"
    Try {
      client.dropTable(dbname, tableName, true, true)
    }

    val config = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName(tableName), Topic("mytopic"), true, true, partitions = Seq(PartitionField("title")), partitioner = StrictPartitionHandler)
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    intercept[RuntimeException] {
      val sink = HiveSink.from(TableName(tableName), config)
      users.foreach(sink.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(1))))
      sink.close()
    }.getMessage shouldBe "Partition 'mr' does not exist and strict policy requires upfront creation"
  }

  it should "evolve the schema by adding a missing field when evolution policy is set to add" in {

    val tableName = "add_evolution_test"

    val schema1 = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().optional().build())
      .build()

    val list1 = List(new Struct(schema1).put("a", "aaa").put("b", "bbb"))

    val config = HiveSinkConfig(DatabaseName(dbname), tableOptions = Set(
      TableOptions(TableName(tableName), Topic("mytopic"), true, true, evolutionPolicy = AddEvolutionPolicy)
    ),
      kerberos = None,
      hadoopConfiguration = HadoopConfiguration.Empty
    )

    // first we write out one row, with fields a,b and then we write out a second row, with an extra
    // field, and then the schema should have been evolved to add the extra field.

    val sink1 = HiveSink.from(TableName(tableName), config)
    list1.foreach(sink1.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(1))))
    sink1.close()

    client.getTable(dbname, tableName).getSd.getCols.asScala.map(_.getName) shouldBe Seq("a", "b")

    val schema2 = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().optional().build())
      .field("x", SchemaBuilder.string().optional().build())
      .build()

    val list2 = List(new Struct(schema2).put("a", "aaaa").put("b", "bbbb").put("x", "xxxx"))

    val sink2 = HiveSink.from(TableName(tableName), config)
    list2.foreach(sink2.write(_, TopicPartitionOffset(Topic("mytopic"), 1, Offset(2))))
    sink2.close()

    client.getTable(dbname, tableName).getSd.getCols.asScala.map(_.getName) shouldBe Seq("a", "b", "x")
  }
}
