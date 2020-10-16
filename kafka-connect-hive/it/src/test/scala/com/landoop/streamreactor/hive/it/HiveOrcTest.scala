package com.landoop.streamreactor.hive.it

import java.time.Duration
import java.time.temporal.ChronoUnit

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.io.Source

class HiveOrcTest extends AnyWordSpec with Matchers with PersonTestData with Eventually with HiveTests {

  private implicit val patience: PatienceConfig = PatienceConfig(Span(120, Seconds), Span(10, Seconds))

  "Hive" should {
    "write non partitioned orc records" in {
      val count = 10000L

      val topic = createTopic()
      val taskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task_no_partitions-orc.json")).getLines().mkString("\n")
        .replace("{{TOPIC}}", topic)
        .replace("{{TABLE}}", topic)
        .replace("{{NAME}}", topic)
      postTask(taskDef)

      val producer = stringStringProducer()
      writeRecords(producer, topic, JacksonSupport.mapper.writeValueAsString(person), count)
      producer.close(Duration.of(30, ChronoUnit.SECONDS))

      // we now should have 10000 records in hive which we can test via jdbc
      eventually {
        withConn { conn =>
          val stmt = conn.createStatement
          val rs = stmt.executeQuery(s"select count(*) from $topic")
          rs.next()
          rs.getLong(1) shouldBe count
        }
      }

      stopTask(topic)
    }

    "write orc with integer partition key" in {
      val count = 1000L

      val topic = createTopic()
      val taskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task_partition_key-orc.json")).getLines().mkString("\n")
        .replace("{{TOPIC}}", topic)
        .replace("{{TABLE}}", topic)
        .replace("{{NAME}}", topic)
      postTask(taskDef)

      val producer = stringStringProducer()

      val studentTestData = new StudentTestData

      writeRecords(producer, topic, JacksonSupport.mapper.writeValueAsString(studentTestData.createStudent), count)
      producer.close(Duration.of(30, ChronoUnit.SECONDS))

      // we now should have 10000 records in hive which we can test via jdbc
      eventually {
        withConn { conn =>
          val stmt = conn.createStatement
          val rs = stmt.executeQuery(s"select count(*) from $topic")
          rs.next()
          rs.getLong(1) shouldBe count
        }
      }

      stopTask(topic)
    }
  }



}
