package com.landoop.streamreactor.hive.it

import java.util.Collections
import java.util.concurrent.TimeUnit

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.io.Source

class HiveSourceTest extends AnyWordSpec with Matchers with PersonTestData with Eventually with HiveTests {

  private implicit val patience: PatienceConfig = PatienceConfig(Span(60000, Millis), Span(5000, Millis))

  "Hive" should {
    "read non partitioned table" in {
      val count = 2000L

      val inputTopic = createTopic()
      val sinkTaskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task_no_partitions.json")).getLines().mkString("\n")
        .replace("{{TOPIC}}", inputTopic)
        .replace("{{TABLE}}", inputTopic)
        .replace("{{NAME}}", inputTopic)
      postTask(sinkTaskDef)

      val producer = stringStringProducer()
      writeRecords(producer, inputTopic, JacksonSupport.mapper.writeValueAsString(person), count)
      producer.close(30, TimeUnit.SECONDS)

      // we now should have 1000 records in hive which we can test via jdbc
      eventually {
        withConn { conn =>
          val stmt = conn.createStatement
          val rs = stmt.executeQuery(s"select count(*) from $inputTopic")
          rs.next()
          rs.getLong(1) shouldBe count
        }
      }

      stopTask(inputTopic)

      // now we can read them back in
      val outputTopic = createTopic()

      val sourceTaskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_source_task.json")).getLines().mkString("\n")
        .replace("{{TOPIC}}", outputTopic)
        .replace("{{TABLE}}", inputTopic)
        .replace("{{NAME}}", outputTopic)
      postTask(sourceTaskDef)

      // we should have 1000 records on the outputTopic
      var records = 0L
      val consumer = stringStringConsumer("earliest")
      consumer.subscribe(Collections.singleton(outputTopic))
      eventually {
        records = records + readRecords(consumer, outputTopic, 2, TimeUnit.SECONDS).size
        records shouldBe count
      }

      stopTask(outputTopic)
    }
  }
}
