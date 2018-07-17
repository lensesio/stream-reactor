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

  val states = Array(
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "District of Columbia",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin"
  )

  case class Person(name: String, state: String, age: Int)

  // write 1 million random records, but with some determinism
  for (_ <- 1 to 1000000) {
    val person = Person(Random.nextString(10), states(Random.nextInt(states.length)), Random.nextInt(99))
    producer.send(new ProducerRecord("hive_sink_topic", JacksonSupport.mapper.writeValueAsString(person)))
  }

  producer.close()
}
