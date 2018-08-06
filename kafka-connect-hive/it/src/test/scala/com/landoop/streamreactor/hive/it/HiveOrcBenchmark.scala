package com.landoop.streamreactor.hive.it

import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.io.Source

object HiveOrcBenchmark extends App with PersonTestData with HiveTests {

  import scala.concurrent.ExecutionContext.Implicits.global

  val count = 10000000000L
  val start = System.currentTimeMillis()

  val topic = createTopic()
  val taskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task_no_partitions-orc.json")).getLines().mkString("\n")
    .replace("{{TOPIC}}", topic)
    .replace("{{TABLE}}", topic)
    .replace("{{NAME}}", topic)
  postTask(taskDef)

  Future {
    val producer = stringStringProducer()
    for (k <- 1 to count.toInt) {
      producer.send(new ProducerRecord(topic, JacksonSupport.mapper.writeValueAsString(person)))
      if (k % 1000 == 0) {
        producer.flush()
      }
    }
    producer.flush()
    producer.close()
  }

  Future {
    var total = 0L
    while (total < count) {
      Thread.sleep(1000)
      withConn { conn =>
        val stmt = conn.createStatement
        val rs = stmt.executeQuery(s"select count(*) from $topic")
        rs.next()
        total = rs.getLong(1)
        val time = System.currentTimeMillis() - start
        println(s"Total $total in ${time}ms")
      }
    }
    stopTask(topic)
  }
}