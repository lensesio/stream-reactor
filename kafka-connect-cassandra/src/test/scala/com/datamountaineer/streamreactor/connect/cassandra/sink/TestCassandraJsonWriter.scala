/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.cassandra.sink

import java.util.UUID

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSink}
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{ConsistencyLevel, Session}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.{Decimal, Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 04/05/16.
  * stream-reactor
  */
@DoNotDiscover
class TestCassandraJsonWriter extends AnyWordSpec with Matchers with MockitoSugar with TestConfig with BeforeAndAfterAll {
  
  val keyspace = "sink"
  val contactPoint = "localhost"
  val userName = "cassandra"
  val password = "cassandra"
  var session : Session = _

  override def beforeAll {
    session = createKeySpace(keyspace ,secure = true, ssl = false)
  }

  override def afterAll(): Unit = {
    session.close()
    session.getCluster.close()
  }
  
  def convertR(record: SinkRecord,
               fields: Map[String, String],
               ignoreFields: Set[String] = Set.empty[String],
               key: Boolean = false): SinkRecord = {
    val value: Struct = if (key) record.key().asInstanceOf[Struct] else record.value.asInstanceOf[Struct]

    if (fields.isEmpty && ignoreFields.isEmpty) {
      record
    } else {
      val currentSchema = if (key) record.keySchema() else record.valueSchema()
      val builder: SchemaBuilder = SchemaBuilder.struct.name(record.topic() + "_extracted")

      //build a new schema for the fields
      if (fields.nonEmpty) {
        fields.foreach({ case (name, alias) =>
          val extractedSchema = currentSchema.field(name)
          builder.field(alias, extractedSchema.schema())
        })
      } else if (ignoreFields.nonEmpty) {
        val ignored = currentSchema.fields().asScala.filterNot(f => ignoreFields.contains(f.name()))
        ignored.foreach(i => builder.field(i.name, i.schema))
      } else {
        currentSchema.fields().asScala.foreach(f => builder.field(f.name(), f.schema()))
      }

      val extractedSchema = builder.build()
      val newStruct = new Struct(extractedSchema)
      fields.foreach({ case (name, alias) => newStruct.put(alias, value.get(name)) })

      new SinkRecord(record.topic(), record.kafkaPartition(), Schema.STRING_SCHEMA, "key", extractedSchema, newStruct,
        record.kafkaOffset())
    }
  }

  "Cassandra JsonWriter should write records to two Cassandra tables" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")
    val table1 = "B" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(id text,
         |int_field1 int,
         |double_field1 double,
         |timestamp_field1 timeuuid,
         |PRIMARY KEY(id,timestamp_field1)) WITH CLUSTERING ORDER BY (timestamp_field1 asc)""".stripMargin)

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table1
         |(id text,
         |int_field2 int,
         |double_field2 double,
         |timestamp_field2 timeuuid,
         |PRIMARY KEY(id,timestamp_field2)) WITH CLUSTERING ORDER BY (timestamp_field2 asc)""".stripMargin)

    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val schema = SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field1", Schema.INT32_SCHEMA)
      .field("int_field2", Schema.INT32_SCHEMA)
      .field("double_field1", Schema.FLOAT64_SCHEMA)
      .field("double_field2", Schema.FLOAT64_SCHEMA)
      .field("timestamp_field1", Schema.STRING_SCHEMA)
      .field("timestamp_field2", Schema.STRING_SCHEMA)
      .build

    val d1 = UUIDs.timeBased().toString
    Thread.sleep(1000)
    val d2 = UUIDs.timeBased().toString

    val struct = new Struct(schema)
      .put("id", "id1")
      .put("int_field1", 11)
      .put("int_field2", 12)
      .put("double_field1", 11.11)
      .put("double_field2", 12.12)
      .put("timestamp_field1", d1)
      .put("timestamp_field2", d2)

    val record = new SinkRecord("TOPICA", 0, null, null, schema, struct, 0, System.currentTimeMillis(), TimestampType.CREATE_TIME)

    //get config
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> s"INSERT INTO $table SELECT id, int_field1, double_field1,timestamp_field1 FROM TOPICA; INSERT INTO $table1 SELECT id, int_field2, double_field2,timestamp_field2 FROM TOPICA",
      CassandraConfigConstants.ERROR_POLICY -> ErrorPolicyEnum.NOOP.toString
    ).asJava

    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(1000)
    //check we can get back what we wrote
    val res1 = session.execute(s"SELECT * FROM $keyspace.$table")
    val list1 = res1.all()
    list1.size() shouldBe 1
    list1.asScala.foreach { r =>
      r.getString("id") shouldBe "id1"
      r.getInt("int_field1") shouldBe 11
      r.getDouble("double_field1") shouldBe 11.11
      r.getUUID("timestamp_field1").toString shouldBe d1
    }
    //check we can get back what we wrote
    val res2 = session.execute(s"SELECT * FROM $keyspace.$table1")
    val list2 = res2.all()
    list2.size() shouldBe 1
    list2.asScala.foreach { r =>
      r.getString("id") shouldBe "id1"
      r.getInt("int_field2") shouldBe 12
      r.getDouble("double_field2") shouldBe 12.12
      r.getUUID("timestamp_field2").toString shouldBe d2
    }

    writer.close()
  }

   "Cassandra JsonWriter should write records from two topics to one Cassandra table" in {
      val table = "A" + UUID.randomUUID().toString.replace("-", "_")

     session.execute(
       s"""
          |CREATE TABLE IF NOT EXISTS $keyspace.$table
          |(id text,
          |int_field1 int,
          |double_field1 double,
          |timestamp_field1 timeuuid,
          |PRIMARY KEY(id,timestamp_field1)) WITH CLUSTERING ORDER BY (timestamp_field1 asc)""".stripMargin)

     val context = mock[SinkTaskContext]
     val assignment = getAssignment
     when(context.assignment()).thenReturn(assignment)
     //get test records
     val schema = SchemaBuilder.struct.name("record")
       .version(1)
       .field("id", Schema.STRING_SCHEMA)
       .field("int_field1", Schema.INT32_SCHEMA)
       .field("double_field1", Schema.FLOAT64_SCHEMA)
       .field("timestamp_field1", Schema.STRING_SCHEMA)
       .build

     val d1 = UUIDs.timeBased().toString
     Thread.sleep(1000)
     val d2 = UUIDs.timeBased().toString

     val struct1 = new Struct(schema)
       .put("id", "id1")
       .put("int_field1", 11)
       .put("double_field1", 11.11)
       .put("timestamp_field1", d1)

     val struct2 = new Struct(schema)
       .put("id", "id2")
       .put("int_field1", 11)
       .put("double_field1", 11.11)
       .put("timestamp_field1", d2)

     val record1 = new SinkRecord("TOPICA", 0, null, null, schema, struct1, 0, System.currentTimeMillis(), TimestampType.CREATE_TIME)
     val record2 = new SinkRecord("TOPICB", 0, null, null, schema, struct2, 0, System.currentTimeMillis(), TimestampType.CREATE_TIME)

     //get config
     val props =  Map(
       CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
       CassandraConfigConstants.KEY_SPACE -> keyspace,
       CassandraConfigConstants.USERNAME -> userName,
       CassandraConfigConstants.PASSWD -> password,
       CassandraConfigConstants.KCQL -> s"INSERT INTO $table SELECT * FROM TOPICA; INSERT INTO $table SELECT * FROM TOPICB",
       CassandraConfigConstants.ERROR_POLICY -> ErrorPolicyEnum.THROW.toString
     ).asJava

     val taskConfig = new CassandraConfigSink(props)

     val writer = CassandraWriter(taskConfig, context)
     writer.write(Seq(record1, record2))
     Thread.sleep(1000)
     //check we can get back what we wrote
     val res1 = session.execute(s"SELECT * FROM $keyspace.$table")
     val list1 = res1.all()
     list1.size() shouldBe 2
     writer.close()
   }

  "Cassandra JsonWriter should write records using nested fields in Cassandra tables - STRING SCHEMA" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(id text,
         |int_field int,
         |double_field double,
         |timestamp_field timeuuid,
         |long_field bigint,
         |PRIMARY KEY(id,timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)""".stripMargin)

    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val schema = SchemaBuilder.string()
    val d = UUIDs.timeBased().toString

    val data =
      s"""
         |{
         |   "id": "id1",
         |   "inner1": {
         |     "a": {
         |       "b": 1
         |     },
         |     "int_field": 1111
         |   },
         |   "inner2":{
         |     "double_field": 1111.22,
         |     "long_field" : 101010,
         |     "timestamp_field":"$d"
         |   },
         |   "f1": 1245,
         |   "f2": true
         |}
        """.stripMargin

    val record = new SinkRecord("TOPIC", 0, null, null, schema, data, 0, System.currentTimeMillis(), TimestampType.CREATE_TIME)

    //get config
    val props =  Map(
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> s"INSERT INTO $table SELECT id, inner1.int_field, inner2.* FROM TOPIC",
      CassandraConfigConstants.ERROR_POLICY -> ErrorPolicyEnum.NOOP.toString
    ).asJava
    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(2000)
    //check we can get back what we wrote
    val res1 = session.execute(s"SELECT * FROM $keyspace.$table")
    val list1 = res1.all()
    list1.size() shouldBe 1
    list1.asScala.foreach { r =>
      r.getString("id") shouldBe "id1"
      r.getInt("int_field") shouldBe 1111
      r.getDouble("double_field") shouldBe 1111.22
      r.getLong("long_field") shouldBe 101010
      r.getUUID("timestamp_field").toString shouldBe d
    }

    writer.close()
  }

  "Cassandra JsonWriter should write records using nested fields in Cassandra tables - STRUCT SCHEMA" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(id text,
         |int_field int,
         |double_field double,
         |timestamp_field timeuuid,
         |long_field bigint,
         |PRIMARY KEY(id,timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)""".stripMargin)

    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records

    val inner1Schema = SchemaBuilder.struct()
      .field("int_field", Schema.INT32_SCHEMA)
      .field("something", Schema.STRING_SCHEMA)

    val inner2Schema = SchemaBuilder.struct()
      .field("double_field", Schema.FLOAT64_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("timestamp_field", Schema.STRING_SCHEMA)

    val schema = SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("inner1", inner1Schema)
      .field("inner2", inner2Schema)
      .field("i1", Schema.INT32_SCHEMA)
      .field("s1", Schema.STRING_SCHEMA)
      .build

    val d = UUIDs.timeBased().toString

    val inner1 = new Struct(inner1Schema)
      .put("int_field", 1111)
      .put("something", "s1")

    val inner2 = new Struct(inner2Schema)
      .put("double_field", 1111.22)
      .put("long_field", 101010L)
      .put("timestamp_field", d)

    val struct = new Struct(schema)
      .put("id", "id1")
      .put("inner1", inner1)
      .put("inner2", inner2)
      .put("i1", 100)
      .put("s1", "something")

    val record = new SinkRecord("TOPIC", 0, null, null, schema, struct, 0, System.currentTimeMillis(), TimestampType.CREATE_TIME)

    //get config
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> s"INSERT INTO $table SELECT id, inner1.int_field, inner2.* FROM TOPIC",
      CassandraConfigConstants.ERROR_POLICY -> ErrorPolicyEnum.NOOP.toString
    ).asJava

    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(2000)
    //check we can get back what we wrote
    val res1 = session.execute(s"SELECT * FROM $keyspace.$table")
    val list1 = res1.all()
    list1.size() shouldBe 1
    list1.asScala.foreach { r =>
      r.getString("id") shouldBe "id1"
      r.getInt("int_field") shouldBe 1111
      r.getDouble("double_field") shouldBe 1111.22
      r.getLong("long_field") shouldBe 101010
      r.getUUID("timestamp_field").toString shouldBe d
    }

    writer.close()
  }

  "Cassandra JsonWriter should write records to Cassandra with field selection" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
      s"id text PRIMARY KEY" +
      s", int_field int" +
      s", long_field bigint" +
      s", string_field text, " +
      s"timeuuid_field timeuuid" +
      s", timestamp_field timestamp)")

    val kcql = s"INSERT INTO $table SELECT id, long_field FROM TOPICA"


    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val testRecords = getTestRecords("TOPICA")
    //get config
    val props =  Map(
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kcql
    ).asJava
    
    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(testRecords)
    Thread.sleep(1000)
    //check we can get back what we wrote
    val res = session.execute(s"SELECT * FROM $keyspace.$table")
    val rs = res.all().asScala

    //check we the columns we wanted
    rs.foreach {
      r => {
        r.getString("id")
        r.getInt("int_field")
        r.getLong("long_field")
        intercept[IllegalArgumentException] {
          r.getString("float_field")
        }
      }
    }

    rs.size shouldBe testRecords.size

    writer.close()
  }

  "Cassandra sink should handle incoming decimal fields" in {
    val schema = SchemaBuilder.struct.name("com.data.mountaineer.cassandra.sink,json.decimaltest")
      .version(1)
      .field("id", Schema.INT32_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .field("timeuuid_field", Schema.STRING_SCHEMA)
      .field("decimal_field", Decimal.schema(4))
      .build

    val dec = new java.math.BigDecimal("1373563.1563")
    val struct = new Struct(schema)
      .put("id", 1)
      .put("int_field", 12)
      .put("long_field", 12L)
      .put("string_field", "foo")
      .put("timeuuid_field", UUIDs.timeBased().toString)
      .put("decimal_field", dec)

    val sinkRecord = new SinkRecord("topica", 0, null, null, schema, struct, 1)
    val convertUtil = new AnyRef with ConverterUtil
    val json = convertUtil.convertValueToJson(convertR(sinkRecord, Map.empty)).toString
    val str = json.toString
    str.contains("\"decimal_field\":1373563.1563")
  }
  
  "Cassandra JsonWriter with Retry should throw Retriable Exception" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")
    val kcql = s"INSERT INTO $table SELECT * FROM TOPICA"

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
      s"id text PRIMARY KEY" +
      s", int_field int" +
      s", long_field bigint" +
      s", string_field text" +
      s", timeuuid_field timeuuid" +
      s", timestamp_field timestamp)")
    
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val testRecords = getTestRecords("TOPICA")
    //get config
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kcql,
      CassandraConfigConstants.ERROR_POLICY -> ErrorPolicyEnum.RETRY.toString,
      CassandraConfigConstants.ERROR_RETRY_INTERVAL->"500"
    ).asJava
    
    val taskConfig = new CassandraConfigSink(props)
    val writer = CassandraWriter(taskConfig, context)


    //drop table in cassandra
    session.execute(s"DROP TABLE IF EXISTS $keyspace.$table")
    intercept[RetriableException] {
      writer.write(testRecords)
    }


    //put back table
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
      s"id text PRIMARY KEY" +
      s", int_field int" +
      s", long_field bigint" +
      s", string_field text" +
      s", timeuuid_field timeuuid" +
      s", timestamp_field timestamp)")

    writer.write(testRecords)
    Thread.sleep(2000)
    //check we can get back what we wrote
    val res = session.execute(s"SELECT * FROM $keyspace.$table")
    res.all().size() shouldBe testRecords.size
    writer.close()
  }

  "Cassandra JsonWriter with Noop should throw Cassandra exception and keep going" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")
    val kcql = s"INSERT INTO $table SELECT * FROM TOPICA"

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
      s"id text PRIMARY KEY" +
      s", int_field int" +
      s", long_field bigint" +
      s", string_field text" +
      s", timeuuid_field timeuuid" +
      s", timestamp_field timestamp)")
    
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val testRecords = getTestRecords("TOPICA")
    //get config
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kcql,
      CassandraConfigConstants.ERROR_POLICY -> ErrorPolicyEnum.NOOP.toString
    ).asJava
    
    val taskConfig = new CassandraConfigSink(props)
    val writer = CassandraWriter(taskConfig, context)

    //drop table in cassandra
    session.execute(s"DROP TABLE IF EXISTS $keyspace.$table")
    Thread.sleep(1000)
    writer.write(testRecords)

    //put back table
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
      s"id text PRIMARY KEY" +
      s", int_field int" +
      s", long_field bigint" +
      s", string_field text" +
      s", timeuuid_field timeuuid" +
      s", timestamp_field timestamp)")
    
    writer.write(testRecords)
    Thread.sleep(1000)
    //check we can get back what we wrote
    val res = session.execute(s"SELECT * FROM $keyspace.$table")
    res.all().size() shouldBe testRecords.size
    writer.close()
  }

  "Cassandra sink should start and write records to Cassandra" in {

      val table = "A" + UUID.randomUUID().toString.replace("-", "_")

      val kcql = s"INSERT INTO $table SELECT * FROM TOPICA"

      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
        s"id text PRIMARY KEY" +
        s", int_field int" +
        s", long_field bigint" +
        s", string_field text" +
        s", timeuuid_field timeuuid" +
        s", timestamp_field timestamp)")

      //mock the context to return our assignment when called
      val context = mock[SinkTaskContext]
      val assignment = getAssignment
      when(context.assignment()).thenReturn(assignment)
      //get test records
      val testRecords = getTestRecords("TOPICA")
      //get config
      val config =  Map(
        CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
        CassandraConfigConstants.KEY_SPACE -> keyspace,
        CassandraConfigConstants.USERNAME -> userName,
        CassandraConfigConstants.PASSWD -> password,
        CassandraConfigConstants.KCQL -> kcql
      ).asJava

      //get task
      val task = new CassandraSinkTask()
      //initialise the tasks context
      task.initialize(context)
      //start task
      task.start(config)
      //simulate the call from Connect
      task.put(testRecords.asJava)
      //stop task
      task.stop()

      //check we can get back what we wrote
      val res = session.execute(s"SELECT * FROM $keyspace.$table")
      res.all().size() shouldBe testRecords.size
  }

  "Cassandra sink should  start and write records to Cassandra using ONE as consistency level" in {

      val table = "A" + UUID.randomUUID().toString.replace("-", "_")
      val kcql = s"INSERT INTO $table SELECT * FROM TOPICA"

      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
        s"id text PRIMARY KEY" +
        s", int_field int" +
        s", long_field bigint" +
        s", string_field text" +
        s", timeuuid_field timeuuid" +
        s", timestamp_field timestamp)")

      //mock the context to return our assignment when called
      val context = mock[SinkTaskContext]
      val assignment = getAssignment
      when(context.assignment()).thenReturn(assignment)
      //get test records
      val testRecords = getTestRecords("TOPICA")
      //get config
      val config = Map(
        CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
        CassandraConfigConstants.KEY_SPACE -> keyspace,
        CassandraConfigConstants.USERNAME -> userName,
        CassandraConfigConstants.PASSWD -> password,
        CassandraConfigConstants.KCQL -> kcql,
        CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG -> ConsistencyLevel.ONE.toString
      ).asJava


      //get task
      val task = new CassandraSinkTask()
      //initialise the tasks context
      task.initialize(context)
      //start task
      task.start(config)
      //simulate the call from Connect
      task.put(testRecords.asJava)
      //stop task
      task.stop()

      //check we can get back what we wrote
      val res = session.execute(s"SELECT * FROM $keyspace.$table")
      res.all().size() shouldBe testRecords.size
  }

  "Cassandra sink should start and write records to Cassandra using TTL" in {
      val table = "A" + UUID.randomUUID().toString.replace("-", "_")
      val table2 = "B" + UUID.randomUUID().toString.replace("-", "_")

      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
        s"id text PRIMARY KEY" +
        s", int_field int" +
        s", long_field bigint" +
        s", string_field text" +
        s", timeuuid_field timeuuid" +
        s", timestamp_field timestamp)")

      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table2 (" +
        s"id text" +
        s", int_field int" +
        s", long_field bigint" +
        s", string_field text" +
        s", timestamp_field timestamp" +
        s", timeuuid_field timeuuid" +
        s", PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")

      val kcql = s"INSERT INTO $table SELECT * FROM topic1 TTL=$TTL;INSERT INTO $table2 SELECT * FROM topic2"

      //mock the context to return our assignment when called
      val context = mock[SinkTaskContext]
      val assignment = getAssignment
      when(context.assignment()).thenReturn(assignment)
      //get test records
      val testRecords1 = getTestRecords("topic1")
      val testRecords2 = getTestRecords("topic2")
      val testRecords = testRecords1 ++ testRecords2
      //get config
      val config =  Map(
        CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
        CassandraConfigConstants.KEY_SPACE -> keyspace,
        CassandraConfigConstants.USERNAME -> userName,
        CassandraConfigConstants.PASSWD -> password,
        CassandraConfigConstants.KCQL -> kcql
      ).asJava

      //get task
      val task = new CassandraSinkTask()
      //initialise the tasks context
      task.initialize(context)
      //start task
      task.start(config)
      //simulate the call from Connect
      task.put(testRecords.asJava)
      //stop task
      task.stop()

      //check we can get back what we wrote
      val res1 = session.execute(s"SELECT * FROM $keyspace.$table")
      val res2 = session.execute(s"SELECT * FROM $keyspace.$table2")
      val key1 = testRecords1.head.value().asInstanceOf[Struct].getString("id")
      val key2 = testRecords2.head.value().asInstanceOf[Struct].getString("id")
      res1.all().size() shouldBe testRecords1.size
      res2.all().size() shouldBe testRecords2.size
      val ttl1 = session.execute(s"SELECT TTL (int_field) FROM $keyspace.$table where id = '$key1'")
      val ttl2 = session.execute(s"SELECT TTL (int_field) FROM $keyspace.$table2 where id = '$key2'")
      val one = ttl1.one().getInt("ttl(int_field)")
      val two = ttl2.one().getInt("ttl(int_field)")
      (one < TTL) shouldBe true
      two shouldBe 0
  }

  "Cassandra JSONWriter should handle deletion of records - Key isPrimitive, INT" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    val kql = s"INSERT INTO $table SELECT id, long_field FROM TOPIC"

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(id int,
         |name text,
         |PRIMARY KEY(id, name)) WITH CLUSTERING ORDER BY (name asc)""".stripMargin)

    val idField = Int.box(UUIDs.timeBased().toString.hashCode)
    val nameField = "Unit Test"

    val insert = session.prepare(s"INSERT INTO $keyspace.$table (id, name) VALUES (?, ?)").bind(idField, nameField)
    session.execute(insert)

    // now run the test...
    val record = new SinkRecord("TOPIC", 0, Schema.INT64_SCHEMA, idField, null, null, 1)

    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)

    //get config
    val props = Map(
      CassandraConfigConstants.DELETE_ROW_STATEMENT -> s"delete from $keyspace.$table where id = ?",
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kql,
      CassandraConfigConstants.DELETE_ROW_ENABLED -> "true"
      ).asJava

    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(1000)

    val validate = session.prepare(s"select * from $keyspace.$table where id = ?").bind(idField)
    val inserted = session.execute(validate)
    // data is in the table...
    (inserted.asScala.isEmpty) shouldBe true
    writer.close()
  }

  "Cassandra JSONWriter should handle deletion of records - Key isPrimitive, STRING and DELETE_ROW_STRUCT_FLDS is empty" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    val kql = s"INSERT INTO $table SELECT id, name FROM TOPIC"

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(id text,
         |name text,
         |PRIMARY KEY(id, name)) WITH CLUSTERING ORDER BY (name asc)""".stripMargin)

    val idField = UUIDs.timeBased().toString
    val nameField = "Unit Test"

    val insert = session.prepare(s"INSERT INTO $keyspace.$table (id, name) VALUES (?, ?)").bind(idField, nameField)
    session.execute(insert)

    // now run the test...
    val record = new SinkRecord("TOPIC", 0, Schema.INT64_SCHEMA, idField, null, null, 1)

    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)

    //get config
    val props = Map(
      CassandraConfigConstants.DELETE_ROW_STATEMENT -> s"delete from $keyspace.$table where id = ?",
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kql,
      CassandraConfigConstants.DELETE_ROW_ENABLED -> "true"
    ).asJava

    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(1000)

    val validate = session.prepare(s"select * from $keyspace.$table where id = ?").bind(idField)
    val inserted = session.execute(validate)
    // data is in the table...
    (inserted.asScala.isEmpty) shouldBe true
    writer.close()
  }

  "Cassandra JSONWriter should handle deletion of records - Key isPrimitive, STRING and DELETE_ROW_STRUCT_FLDS is non-empty" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")
    val kql = s"INSERT INTO $table SELECT id, long_field FROM TOPIC"

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(key1 int,
         |key2 text,
         |name text,
         |PRIMARY KEY((key1, key2), name)) WITH CLUSTERING ORDER BY (name asc)""".stripMargin)

    val uuid = UUIDs.timeBased()
    val key1 = Int.box(uuid.hashCode)
    val key2 = uuid.toString
    val name = "Unit Test"

    val insert = session.prepare(s"INSERT INTO $keyspace.$table (key1, key2, name) VALUES (?,?,?)").bind(key1, key2, name)
    session.execute(insert)

    val keySchema = SchemaBuilder.string().build
    val keyValue =
      s"""
         | {
         |   \"key1\": $key1,
         |   \"key2\": \"$key2\"
         | }
       """.stripMargin

    val record = new SinkRecord("TOPIC", 0, keySchema, keyValue, null, null, 1)
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)

    //get config
    val props = Map(
      CassandraConfigConstants.DELETE_ROW_STATEMENT -> s"delete from $keyspace.$table where key1 = ? AND key2 = ?",
      CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS -> s"key1,key2",
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kql,
      CassandraConfigConstants.DELETE_ROW_ENABLED -> "true"
      ).asJava

    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(1000)


    val validate = session.prepare(s"SELECT * FROM $keyspace.$table WHERE key1 = ? AND key2 = ?").bind(key1, key2)
    val result = session.execute(validate)

    (result.asScala.isEmpty) shouldBe true
    writer.close()
  }

  "Cassandra JSONWriter should handle deletion of records - Key isPrimitive, STRING with Complex Type" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")
    val kql = s"INSERT INTO $table SELECT id, long_field FROM TOPIC"

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(key1 int,
         |key2 text,
         |name text,
         |PRIMARY KEY((key1, key2), name)) WITH CLUSTERING ORDER BY (name asc)""".stripMargin)

    val uuid = UUIDs.timeBased()
    val key1 = uuid.hashCode
    val key2 = uuid.toString
    val name = "Unit Test"

    val insert = session.prepare(s"INSERT INTO $keyspace.$table (key1, key2, name) VALUES (?,?,?)").bind(Int.box(key1), key2, name)
    session.execute(insert)

    val keySchema = SchemaBuilder.string().build
    val keyValue =
      s"""
         | {
         |   \"key1\": $key1,
         |   \"nested\": {
         |     \"key2\": \"$key2\"
         |   }
         | }
       """.stripMargin

    val record = new SinkRecord("TOPIC", 0, keySchema, keyValue, null, null, 1)
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)

    //get config
    val props = Map(
      CassandraConfigConstants.DELETE_ROW_STATEMENT -> s"delete from $keyspace.$table where key1 = ? AND key2 = ?",
      CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS -> s"key1,nested.key2",
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kql,
      CassandraConfigConstants.DELETE_ROW_ENABLED -> "true"
      ).asJava

    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(1000)


    val validate = session.prepare(s"SELECT * FROM $keyspace.$table WHERE key1 = ? AND key2 = ?").bind(Int.box(key1), key2)
    val result = session.execute(validate)

    (result.asScala.isEmpty) shouldBe true
    writer.close()
  }


  "Cassandra JSONWriter should handle deletion of records - Key is STRUCT, flat" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")
    val kql = s"INSERT INTO $table SELECT id, long_field FROM TOPIC"

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(key1 int,
         |key2 text,
         |name text,
         |PRIMARY KEY((key1, key2), name)) WITH CLUSTERING ORDER BY (name asc)""".stripMargin)

    val uuid = UUIDs.timeBased()
    val key1 = Int.box(uuid.hashCode)
    val key2 = uuid.toString
    val name = "Unit Test"

    val insert = session.prepare(s"INSERT INTO $keyspace.$table (key1, key2, name) VALUES (?,?,?)").bind(key1, key2, name)
    session.execute(insert)

    val keySchema = SchemaBuilder.struct
        .field("key1", Schema.INT32_SCHEMA)
        .field("key2", Schema.STRING_SCHEMA)
        .build
    val keyStruct = new Struct(keySchema)
    keyStruct.put("key1", key1)
    keyStruct.put("key2", key2)

    val record = new SinkRecord("TOPIC", 0, keySchema, keyStruct, null, null, 1)
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)

    //get config
    val props = Map(
      CassandraConfigConstants.DELETE_ROW_STATEMENT -> s"delete from $keyspace.$table where key1 = ? AND key2 = ?",
      CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS -> s"key1,key2",
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kql,
      CassandraConfigConstants.DELETE_ROW_ENABLED -> "true"
      ).asJava

    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(1000)


    val validate = session.prepare(s"SELECT * FROM $keyspace.$table WHERE key1 = ? AND key2 = ?").bind(key1, key2)
    val result = session.execute(validate)

    (result.asScala.isEmpty) shouldBe true
    writer.close()
  }

  "Cassandra JSONWriter should handle deletion of records - Key is STRUCT, flat, long as key, but in range of int" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")
    val kql = s"INSERT INTO $table SELECT id, long_field FROM TOPIC"

    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table
         |(key bigint,
         |name text,
         |PRIMARY KEY((key), name)) WITH CLUSTERING ORDER BY (name asc)""".stripMargin)

    val uuid = UUIDs.timeBased()
    val key: java.lang.Long = Int.box(uuid.hashCode).toLong
    val name = "Unit Test"

    val insert = session.prepare(s"INSERT INTO $keyspace.$table (key, name) VALUES (?, ?)").bind(key, name)
    session.execute(insert)

    val keySchema = SchemaBuilder.struct
      .field("key", Schema.INT64_SCHEMA)
      .build
    val keyStruct = new Struct(keySchema)
    keyStruct.put("key", key)

    val record = new SinkRecord("TOPIC", 0, keySchema, keyStruct, null, null, 1)
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)

    //get config
    val props = Map(
      CassandraConfigConstants.DELETE_ROW_STATEMENT -> s"delete from $keyspace.$table where key = ?",
      CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS -> s"key",
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> kql,
      CassandraConfigConstants.DELETE_ROW_ENABLED -> "true"
    ).asJava

    val taskConfig = new CassandraConfigSink(props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(Seq(record))
    Thread.sleep(1000)


    val validate = session.prepare(s"SELECT * FROM $keyspace.$table WHERE key = ?").bind(key)
    val result = session.execute(validate)

    (result.asScala.isEmpty) shouldBe true
    writer.close()
  }

}
