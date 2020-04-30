package com.landoop.streamreactor.hive.it

import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.io.Source

class HiveParquetWithPartitionTest extends AnyWordSpec with Matchers with PersonTestData with Eventually with HiveTests {

  private implicit val patience: PatienceConfig = PatienceConfig(Span(60000, Millis), Span(5000, Millis))

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

      // check for the presence of each partition directory
      val table = metastore.getTable("default", topic)
      for (state <- states) {
        fs.exists(new Path(table.getSd.getLocation, s"state=$state")) shouldBe true
      }

      stopTask(topic)
    }
  }
}
