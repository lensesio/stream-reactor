package com.landoop.streamreactor.connect.hive

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  * Writes a bunch of records to a kafka topic, which should be being picked up
  * by a kafka connect process. The hive sink connector will then write those
  * to hive and we can then assert the records have been written correctly.
  */
class HiveSinkIntTest {

  val props = new Properties()
  val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)



  case class Person(name: String, state: String, age: Int)

  // write 1 million random records, but with some determinism
  for (_ <- 1 to 1000000) {
    producer.send(new ProducerRecord("hive_sink_topic", JacksonSupport.mapper.writeValueAsString(person)))
  }

  producer.close()
}
