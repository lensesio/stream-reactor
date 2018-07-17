package com.landoop.streamreactor.hive.it

import java.util.concurrent.TimeUnit

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class HiveWithPartitionTest extends WordSpec with Matchers with TestData with Eventually with HiveTests {

  private implicit val patience = PatienceConfig(Span(60000, Millis), Span(5000, Millis))

  "Hive" should {
    "write partitioned records" in {

      val count = 100000L

      val topic = createTopic()
      val taskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task_with_partitions.json")).getLines().mkString("\n")
        .replace("{{TOPIC}}", topic)
        .replace("{{TABLE}}", topic)
        .replace("{{NAME}}", topic)
      postTask(taskDef)

      val producer = stringStringProducer()
      writeRecords(producer, topic, JacksonSupport.mapper.writeValueAsString(person), count)
      producer.close(30, TimeUnit.SECONDS)

      // wait for some data to have been flushed
      eventually {
        withConn { conn =>
          val stmt = conn.createStatement
          val rs = stmt.executeQuery(s"select count(*) FROM $topic")
          if (rs.next()) {
            val count = rs.getLong(1)
            println(s"Current count for $topic is $count")
            count should be > 100L
          } else {
            fail()
          }
        }
      }

      // we should see every partition created
      eventually {
        withConn { conn =>
          val stmt = conn.createStatement
          val rs = stmt.executeQuery(s"select distinct state from $topic")
          var count = 0
          while (rs.next()) {
            count = count + 1
          }
          println(s"State count is $count")
          count shouldBe states.length
        }
      }

      stopTask(topic)
    }
  }
}
