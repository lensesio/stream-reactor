/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.utils.ITSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Base64
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class S3SinkTaskJsonEnvelopeTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with MockitoSugar
    with LazyLogging {

  import helper._

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "myTopic"

  private def DefaultProps = Map(
    AWS_ACCESS_KEY              -> Identity,
    AWS_SECRET_KEY              -> Credential,
    AUTH_MODE                   -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT             -> uri(),
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    "name"                      -> "s3SinkTaskBuildLocalTest",
    AWS_REGION                  -> "eu-west-1",
    TASK_INDEX                  -> "1:1",
  )

  private def toSinkRecord(
    user:      Struct,
    topic:     String,
    partition: Int,
    offset:    Long,
    timestamp: Long,
    headers:   (String, SchemaAndValue)*,
  ) = {
    val record =
      new SinkRecord(topic, partition, schema, user, schema, user, offset, timestamp, TimestampType.CREATE_TIME)
    headers.foreach {
      case (name, schemaAndValue) =>
        record.headers().add(name, schemaAndValue)
    }
    record
  }

  private def toSinkRecord(
    value:     Any,
    topic:     String,
    partition: Int,
    offset:    Long,
    timestamp: Long,
    headers:   (String, SchemaAndValue)*,
  ) = {
    val record =
      new SinkRecord(topic, partition, null, value, null, value, offset, timestamp, TimestampType.CREATE_TIME)
    headers.foreach {
      case (name, schemaAndValue) =>
        record.headers().add(name, schemaAndValue)
    }
    record
  }

  "S3SinkTask" should "write to json format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON` WITH_FLUSH_COUNT = 3 PROPERTIES('store.envelope'=true,'padding.length.partition'='12', 'padding.length.offset'='12')",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val struct1 = new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43)
    val struct2 = new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06)
    val struct3 = new Struct(schema).put("name", "tom").put("title", null).put("salary", 395.44)

    val record1 = toSinkRecord(
      struct1,
      TopicName,
      1,
      1L,
      10001L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 1L),
    )
    val record2 = toSinkRecord(
      struct2,
      TopicName,
      1,
      2L,
      10002L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )
    val record3 = toSinkRecord(
      struct3,
      TopicName,
      1,
      3L,
      10003L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 3L),
    )
    task.put(List(record1, record2, record3).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.json")

    val jsonRecords = new String(bytes).split("\n")
    jsonRecords.size should be(3)

    val actual1 = jsonRecords.head
    actual1 should be(
      """{"headers":{"h1":"record1-header1","h2":1},"metadata":{"partition":1,"offset":1,"topic":"myTopic","timestamp":10001},"value":{"name":"sam","title":"mr","salary":100.43},"key":{"name":"sam","title":"mr","salary":100.43}}""",
    )

    val actual2 = jsonRecords(1)
    actual2 should be(
      """{"headers":{"h1":"record1-header2","h2":2},"metadata":{"partition":1,"offset":2,"topic":"myTopic","timestamp":10002},"value":{"name":"laura","title":"ms","salary":429.06},"key":{"name":"laura","title":"ms","salary":429.06}}""",
    )

    val actual3 = jsonRecords(2)
    actual3 should be(
      """{"headers":{"h1":"record1-header3","h2":3},"metadata":{"partition":1,"offset":3,"topic":"myTopic","timestamp":10003},"value":{"name":"tom","title":null,"salary":395.44},"key":{"name":"tom","title":null,"salary":395.44}}""",
    )
  }

  "S3SinkTask" should "write to JSON format when input is java Map" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON`  WITH_FLUSH_COUNT = 3 PROPERTIES('store.envelope'=true,'padding.length.partition'='12', 'padding.length.offset'='12')",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val map1 = Map[String, Any]("name" -> "sam", "title" -> "mr", "salary" -> 100.43).asJava
    val map2 = Map[String, Any]("name" -> "laura", "title" -> "ms", "salary" -> 429.06).asJava
    val map3 = Map[String, Any]("name" -> "tom", "title" -> null.asInstanceOf[String], "salary" -> 395.44).asJava

    val record1 = toSinkRecord(
      map1,
      TopicName,
      1,
      1L,
      10001L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 1L),
    )
    val record2 = toSinkRecord(
      map2,
      TopicName,
      1,
      2L,
      10002L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )
    val record3 = toSinkRecord(
      map3,
      TopicName,
      1,
      3L,
      10003L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 3L),
    )
    task.put(List(record1, record2, record3).asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")

    files.size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.json")

    val jsonRecords = new String(bytes).split("\n")
    jsonRecords.size should be(3)

    val actual1 = jsonRecords.head
    actual1 should be(
      """{"headers":{"h1":"record1-header1","h2":1},"metadata":{"partition":1,"offset":1,"topic":"myTopic","timestamp":10001},"value":{"name":"sam","salary":100.43,"title":"mr"},"key":{"name":"sam","salary":100.43,"title":"mr"}}""",
    )

    val actual2 = jsonRecords(1)
    actual2 should be(
      """{"headers":{"h1":"record1-header2","h2":2},"metadata":{"partition":1,"offset":2,"topic":"myTopic","timestamp":10002},"value":{"name":"laura","salary":429.06,"title":"ms"},"key":{"name":"laura","salary":429.06,"title":"ms"}}""",
    )

    val actual3 = jsonRecords(2)
    actual3 should be(
      """{"headers":{"h1":"record1-header3","h2":3},"metadata":{"partition":1,"offset":3,"topic":"myTopic","timestamp":10003},"value":{"name":"tom","salary":395.44,"title":null},"key":{"name":"tom","salary":395.44,"title":null}}""",
    )
  }

  "S3SinkTask" should "write to JSON format when input is java Map and escape new line text" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON`  WITH_FLUSH_COUNT = 3 PROPERTIES('store.envelope'=true,'padding.length.partition'='12', 'padding.length.offset'='12')",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val map1 = Map[String, Any]("name" -> "samuel\n jackson", "title" -> "mr", "salary" -> "100.43").asJava
    val map2 = Map[String, Any]("name" -> "anna\nkarenina", "title" -> "ms", "salary" -> "429.06").asJava
    val map3 =
      Map[String, Any]("name" -> "tom\nhardy", "title" -> null.asInstanceOf[String], "salary" -> "395.44").asJava

    val record1 = toSinkRecord(
      map1,
      TopicName,
      1,
      1L,
      10001L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 1L),
    )
    val record2 = toSinkRecord(
      map2,
      TopicName,
      1,
      2L,
      10002L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )
    val record3 = toSinkRecord(
      map3,
      TopicName,
      1,
      3L,
      10003L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 3L),
    )
    task.put(List(record1, record2, record3).asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")

    files.size should be(1)

    val bytes       = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.json")
    val jsonRecords = new String(bytes).split("\n")
    jsonRecords.size should be(3)

    val actual1 = jsonRecords.head

    actual1 should be(
      """{"headers":{"h1":"record1-header1","h2":1},"metadata":{"partition":1,"offset":1,"topic":"myTopic","timestamp":10001},"value":{"name":"samuel\n jackson","salary":"100.43","title":"mr"},"key":{"name":"samuel\n jackson","salary":"100.43","title":"mr"}}""",
    )

    val actual2 = jsonRecords(1)
    actual2 should be(
      """{"headers":{"h1":"record1-header2","h2":2},"metadata":{"partition":1,"offset":2,"topic":"myTopic","timestamp":10002},"value":{"name":"anna\nkarenina","salary":"429.06","title":"ms"},"key":{"name":"anna\nkarenina","salary":"429.06","title":"ms"}}""",
    )

    val actual3 = jsonRecords(2)
    actual3 should be(
      """{"headers":{"h1":"record1-header3","h2":3},"metadata":{"partition":1,"offset":3,"topic":"myTopic","timestamp":10003},"value":{"name":"tom\nhardy","salary":"395.44","title":null},"key":{"name":"tom\nhardy","salary":"395.44","title":null}}""",
    )
  }

  "S3SinkTask" should "write to JSON format when input is java Map escapes new line when envelope is disabled" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON`  WITH_FLUSH_COUNT = 3 PROPERTIES('store.envelope'=false,'padding.length.partition'='12', 'padding.length.offset'='12')",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val map1 = Map[String, Any]("name" -> "samuel\n jackson", "title" -> "mr", "salary" -> "100.43").asJava
    val map2 = Map[String, Any]("name" -> "anna\nkarenina", "title" -> "ms", "salary" -> "429.06").asJava
    val map3 =
      Map[String, Any]("name" -> "tom\nhardy", "title" -> null.asInstanceOf[String], "salary" -> "395.44").asJava

    val record1 = toSinkRecord(
      map1,
      TopicName,
      1,
      1L,
      10001L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 1L),
    )
    val record2 = toSinkRecord(
      map2,
      TopicName,
      1,
      2L,
      10002L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )
    val record3 = toSinkRecord(
      map3,
      TopicName,
      1,
      3L,
      10003L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 3L),
    )
    task.put(List(record1, record2, record3).asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")
    files.size should be(1)

    val bytes       = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.json")
    val jsonRecords = new String(bytes).split("\n")
    jsonRecords.size should be(3)

    val actual1 = jsonRecords.head

    actual1 shouldBe """{"name":"samuel\n jackson","title":"mr","salary":"100.43"}"""

    val actual2 = jsonRecords(1)
    actual2 shouldBe """{"name":"anna\nkarenina","title":"ms","salary":"429.06"}"""

    val actual3 = jsonRecords(2)
    actual3 shouldBe """{"name":"tom\nhardy","title":null,"salary":"395.44"}"""
  }

  "S3SinkTask" should "write to JSON format when input is array of bytes representing text with new line" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON`  WITH_FLUSH_COUNT = 3 PROPERTIES('store.envelope'=true,'padding.length.partition'='12', 'padding.length.offset'='12')",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val array1 = "samuel jackson".getBytes
    val array2 = "anna\nkarenina".getBytes
    val array3 = "tom\nhardy".getBytes

    val record1 = toSinkRecord(
      array1,
      TopicName,
      1,
      1L,
      10001L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 1L),
    )
    val record2 = toSinkRecord(
      array2,
      TopicName,
      1,
      2L,
      10002L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )
    val record3 = toSinkRecord(
      array3,
      TopicName,
      1,
      3L,
      10003L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 3L),
    )
    task.put(List(record1, record2, record3).asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")

    files.size should be(1)

    val bytes       = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.json")
    val jsonRecords = new String(bytes).split("\n")
    jsonRecords.size should be(3)

    val actual1 = jsonRecords.head
    //convert actual1 string to Json using Jackson
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val json1     = objectMapper.readTree(actual1)
    val json1Node = json1.get("key")
    json1Node should not be null
    json1Node.isArray shouldBe false
    new String(Base64.getDecoder.decode(json1Node.asText())) shouldBe "samuel jackson"
    json1.get("keyIsArray").asBoolean() shouldBe true
    val json1NodeValue = json1.get("value")
    json1NodeValue should not be null
    json1NodeValue.isArray shouldBe false
    new String(Base64.getDecoder.decode(json1NodeValue.asText())) shouldBe "samuel jackson"
    json1.has("valueIsArray") shouldBe true

    val actual2   = jsonRecords(1)
    val json2     = objectMapper.readTree(actual2)
    val json2Node = json2.get("key")
    json2Node should not be null
    json2Node.isArray shouldBe false
    json2.get("keyIsArray").asBoolean() shouldBe true
    new String(Base64.getDecoder.decode(json2Node.asText())) shouldBe "anna\nkarenina"
    val json2NodeValue = json2.get("value")
    json2NodeValue should not be null
    json2NodeValue.isArray shouldBe false
    json2.get("valueIsArray").asBoolean() shouldBe true
    new String(Base64.getDecoder.decode(json2NodeValue.asText())) shouldBe "anna\nkarenina"

    val actual3   = jsonRecords(2)
    val json3     = objectMapper.readTree(actual3)
    val json3Node = json3.get("key")
    json3Node should not be null
    json3Node.isArray shouldBe false
    json3.get("keyIsArray").asBoolean() shouldBe true
    new String(Base64.getDecoder.decode(json3Node.asText())) shouldBe "tom\nhardy"
    val json3NodeValue = json3.get("value")
    json3NodeValue should not be null
    json3NodeValue.isArray shouldBe false
    json3.get("valueIsArray").asBoolean() shouldBe true
    new String(Base64.getDecoder.decode(json3NodeValue.asText())) shouldBe "tom\nhardy"
  }

}
