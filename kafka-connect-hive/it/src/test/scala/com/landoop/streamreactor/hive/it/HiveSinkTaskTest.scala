package com.landoop.streamreactor.connect.hive.it

import com.landoop.streamreactor.connect.hive.sink.HiveSinkTask
import com.landoop.streamreactor.connect.hive.sink.config.SinkConfigSettings
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava, SeqHasAsJava}
import scala.util.Try

class HiveSinkTaskTest extends AnyFlatSpec with Matchers with HiveTestConfig {

  val schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", SchemaBuilder.float64().optional().build())
    .build()

  val dbname = "hive_sink_task_test"


  "HiveSinkTask" should "write to a partitioned table" in {

    implicit val (client, fs) = testInit(dbname)

    val table = "employees"

    val users = List(
      new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06),
      new Struct(schema).put("name", "tom").put("title", "mr").put("salary", 395.44)
    )

    val records = users.zipWithIndex.map { case (user, k) =>
      new SinkRecord("mytopic", 1, null, null, schema, user, k.toLong)
    }

    Try {
      client.dropTable(dbname, table, true, true)
    }

    // table should not exist
    intercept[NoSuchObjectException] {
      client.getTable(dbname, table)
    }

    val task = new HiveSinkTask(fs, client)

    // kafka connect will invoke start as the first part of the lifecycle
    // we mimic this with the example config
    import SinkConfigSettings._

    val props = Map(
      DatabaseNameKey -> dbname,
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
    val t = client.getTable(dbname, table)
    t.getPartitionKeys.asScala.map(_.getName) shouldBe Seq("title")
    t.getSd.getCols.asScala.map(_.getName) shouldBe Seq("name", "salary")

    // each partition should have a file
    fs.listFiles(new Path(t.getSd.getLocation, "title=mr"), true).next().getPath.toString shouldBe
      s"hdfs://localhost:8020/user/hive/warehouse/$dbname/employees/title=mr/streamreactor_mytopic_1_0"

    fs.listFiles(new Path(t.getSd.getLocation, "title=ms"), true).next().getPath.toString shouldBe
      s"hdfs://localhost:8020/user/hive/warehouse/$dbname/employees/title=ms/streamreactor_mytopic_1_1"
  }

  // todo
  it should "not re-write data after a rebalance" ignore {

    implicit val (client, fs) = testInit(dbname)

    val table = "employees"

    val users = List(
      new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06),
      new Struct(schema).put("name", "tom").put("title", "mr").put("salary", 395.44)
    )

    val records = users.zipWithIndex.map { case (user, k) =>
      new SinkRecord("mytopic", 1, null, null, schema, user, k.toLong)
    }

    Try {
      client.dropTable(dbname, table, true, true)
    }

    // table should not exist
    intercept[NoSuchObjectException] {
      client.getTable(dbname, table)
    }

    val task = new HiveSinkTask(fs, client)

    // kafka connect will invoke start as the first part of the lifecycle
    // we mimic this with the example config
    import SinkConfigSettings._

    val props = Map(
      DatabaseNameKey -> dbname
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
    val t = client.getTable(dbname, table)
    t.getPartitionKeys.asScala.map(_.getName) shouldBe Seq("title")
    t.getSd.getCols.asScala.map(_.getName) shouldBe Seq("name", "salary")

    // each partition should have a file
    fs.listFiles(new Path(t.getSd.getLocation, "title=mr"), true).next().getPath.toString shouldBe
      s"hdfs://localhost:8020/user/hive/warehouse/$dbname/employees/title=mr/streamreactor_mytopic_1_0"

    fs.listFiles(new Path(t.getSd.getLocation, "title=ms"), true).next().getPath.toString shouldBe
      s"hdfs://localhost:8020/user/hive/warehouse/$dbname/employees/title=ms/streamreactor_mytopic_1_1"
  }
}
