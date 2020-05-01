package com.landoop.streamreactor.connect.hive.sink

import java.util

import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.config.SinkConfigSettings
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.{Database, NoSuchObjectException}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.util.Try

class HiveSinkTaskTest extends AnyFlatSpec with Matchers with HiveTestConfig {

  val schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", SchemaBuilder.float64().optional().build())
    .build()

  val db = "hive_sink_task_test"

  Try {
    client.dropDatabase(db, true, true)
  }

  Try {
    client.createDatabase(new Database(db, null, s"/user/hive/warehouse/$db", new util.HashMap()))
  }

  "HiveSinkTask" should "write to a partitioned table" in {

    val table = "employees"

    val users = List(
      new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06),
      new Struct(schema).put("name", "tom").put("title", "mr").put("salary", 395.44)
    )

    val records = users.zipWithIndex.map { case (user, k) =>
      new SinkRecord("mytopic", 1, null, null, schema, user, k)
    }

    Try {
      client.dropTable(db, table, true, true)
    }

    // table should not exist
    intercept[NoSuchObjectException] {
      client.getTable(db, table)
    }

    val task = new HiveSinkTask(fs, client)

    // kafka connect will invoke start as the first part of the lifecycle
    // we mimic this with the example config
    import SinkConfigSettings._

    val props = Map(
      DatabaseNameKey -> db,
      "connect.hive.kcql" -> "insert into employees select * from mytopic"
    ).asJava

    task.start(props)

    // next the list of topics will be sent via open
    task.open(Seq(new TopicPartition("mytopic", 1)).asJava)

    // writer some records
    task.put(records.asJava)

    // close will force a flush
    task.close(Seq(new TopicPartition("mytopic", 1)).asJava)

    // end the task
    task.stop()

    // table should have been created with data fields and partition field
    val t = client.getTable(db, table)
    t.getPartitionKeys.asScala.map(_.getName) shouldBe Seq("title")
    t.getSd.getCols.asScala.map(_.getName) shouldBe Seq("name", "salary")

    // each partition should have a file
    fs.listFiles(new Path(t.getSd.getLocation, "title=mr"), true).next().getPath.toString shouldBe
      s"hdfs://localhost:8020/user/hive/warehouse/$db/employees/title=mr/streamreactor_mytopic_1_0"

    fs.listFiles(new Path(t.getSd.getLocation, "title=ms"), true).next().getPath.toString shouldBe
      s"hdfs://localhost:8020/user/hive/warehouse/$db/employees/title=ms/streamreactor_mytopic_1_1"
  }

  // todo
  it should "not re-write data after a rebalance" ignore {

    val table = "employees"

    val users = List(
      new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06),
      new Struct(schema).put("name", "tom").put("title", "mr").put("salary", 395.44)
    )

    val records = users.zipWithIndex.map { case (user, k) =>
      new SinkRecord("mytopic", 1, null, null, schema, user, k)
    }

    Try {
      client.dropTable(db, table, true, true)
    }

    // table should not exist
    intercept[NoSuchObjectException] {
      client.getTable(db, table)
    }

    val task = new HiveSinkTask(fs, client)

    // kafka connect will invoke start as the first part of the lifecycle
    // we mimic this with the example config
    import SinkConfigSettings._

    val props = Map(
      DatabaseNameKey -> db
    ).asJava

    task.start(props)

    // next the list of topics will be sent via open
    task.open(Seq(new TopicPartition("mytopic", 1)).asJava)

    // writer some records
    task.put(records.asJava)

    // close will force a flush
    task.close(Seq(new TopicPartition("mytopic", 1)).asJava)

    // end the task
    task.stop()

    // table should have been created with data fields and partition field
    val t = client.getTable(db, table)
    t.getPartitionKeys.asScala.map(_.getName) shouldBe Seq("title")
    t.getSd.getCols.asScala.map(_.getName) shouldBe Seq("name", "salary")

    // each partition should have a file
    fs.listFiles(new Path(t.getSd.getLocation, "title=mr"), true).next().getPath.toString shouldBe
      s"hdfs://localhost:8020/user/hive/warehouse/$db/employees/title=mr/streamreactor_mytopic_1_0"

    fs.listFiles(new Path(t.getSd.getLocation, "title=ms"), true).next().getPath.toString shouldBe
      s"hdfs://localhost:8020/user/hive/warehouse/$db/employees/title=ms/streamreactor_mytopic_1_1"
  }
}