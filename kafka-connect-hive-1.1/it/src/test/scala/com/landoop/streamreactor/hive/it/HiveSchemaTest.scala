package com.landoop.streamreactor.hive.it

import java.util.concurrent.TimeUnit

import com.landoop.streamreactor.connect.hive.{DatabaseName, TableName}
import org.apache.kafka.connect.data.Schema
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source
import scala.util.Random
import scala.collection.JavaConverters._

class HiveSchemaTest extends WordSpec with Matchers with PersonTestData with Eventually with HiveTests {

  private implicit val patience: PatienceConfig = PatienceConfig(Span(60000, Millis), Span(5000, Millis))

  case class Foo(s: String, l: Long, b: Boolean, d: Double)
  def foo = Foo("string", Random.nextLong, Random.nextBoolean, Random.nextDouble)

  "Hive" should {
    "create correct schema for table" in {

      val topic = createTopic()
      val taskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task_no_partitions.json")).getLines().mkString("\n")
        .replace("{{TOPIC}}", topic)
        .replace("{{TABLE}}", topic)
        .replace("{{NAME}}", topic)
      postTask(taskDef)

      val producer = stringStringProducer()
      writeRecords(producer, topic, JacksonSupport.mapper.writeValueAsString(foo), 2000)
      producer.close(30, TimeUnit.SECONDS)

      // wait for some data to have been flushed
      eventually {
        withConn { conn =>
          val stmt = conn.createStatement
          val rs = stmt.executeQuery(s"select count(*) FROM $topic")
          rs.next()
          rs.getLong(1) should be > 0L
        }
      }

      // check that the schema is correct
      val schema = com.landoop.streamreactor.connect.hive.schema(DatabaseName("default"), TableName(topic))
      schema.fields().asScala.map(_.name).toSet shouldBe Set("s", "b", "l", "d")
      schema.field("s").schema().`type`() shouldBe Schema.Type.STRING
      schema.field("l").schema().`type`() shouldBe Schema.Type.INT64
      schema.field("d").schema().`type`() shouldBe Schema.Type.FLOAT64
      schema.field("b").schema().`type`() shouldBe Schema.Type.BOOLEAN

      stopTask(topic)
    }
  }
}
