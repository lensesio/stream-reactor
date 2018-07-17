package com.landoop.streamreactor.hive.it

import java.util.concurrent.TimeUnit

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class HiveNoPartitionTest extends WordSpec with Matchers with TestData with Eventually with HiveTests {

  private implicit val patience = PatienceConfig(Span(30000, Millis), Span(2000, Millis))

  "Hive" should {
    "write records" in {

      val count = 10000L

      val topic = createTopic()
      val taskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task_no_partitions.json")).getLines().mkString("\n")
        .replace("{{TOPIC}}", topic)
        .replace("{{TABLE}}", topic)
        .replace("{{NAME}}", topic)
      postTask(taskDef)

      val producer = stringStringProducer()
      writeRecords(producer, topic, JacksonSupport.mapper.writeValueAsString(person), count)
      producer.close(30, TimeUnit.SECONDS)

      // we now should have 1000 records in hive which we can test via jdbc
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
