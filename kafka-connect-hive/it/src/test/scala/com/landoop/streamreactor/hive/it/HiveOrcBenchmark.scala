package com.landoop.streamreactor.hive.it

import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.io.Source
import scala.util.Try

object HiveOrcBenchmark extends App with PersonTestData with HiveTests {

  import scala.concurrent.ExecutionContext.Implicits.global

  val start = System.currentTimeMillis()

  val topic = createTopic()
  val taskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task_no_partitions-orc.json")).getLines().mkString("\n")
    .replace("{{TOPIC}}", topic)
    .replace("{{TABLE}}", topic)
    .replace("{{NAME}}", topic)
  postTask(taskDef)

  Future {
    val producer = stringStringProducer()
    val count = 10000000 // 10mil
    for (k <- 0 until count) {
      producer.send(new ProducerRecord(topic, JacksonSupport.mapper.writeValueAsString(person)))
      if (k % 100000 == 0) {
        println(s"Flushing records [total=$count]")
        producer.flush()
      }
    }
    producer.flush()
    producer.close()
  }

  Future {
    while (true) {
      Try {
        Thread.sleep(2000)
        withConn { conn =>
          val stmt = conn.createStatement
          val rs = stmt.executeQuery(s"select count(*) from $topic")
          rs.next()
          val total = rs.getLong(1)
          val time = System.currentTimeMillis() - start
          println(s"Total $total in ${time}ms which is ${total / (time / 1000)} records per second")
        }
      }
    }
    stopTask(topic)
  }
}