package com.landoop.streamreactor.hive.it

import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.hive.jdbc.HiveDriver
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.asynchttpclient.Dsl
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.{Random, Try}

class HiveNoPartitionTest extends WordSpec with Matchers with TestData with Eventually {
  new HiveDriver()

  private val client = Dsl.asyncHttpClient(Dsl.config())

  private val admin = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://127.0.0.1:9092")
    AdminClient.create(props)
  }

  private implicit val patience = PatienceConfig(Span(30000, Millis), Span(2000, Millis))

  "Hive" should {
    "write records" in {

      // use the same name for the topic, table in hive, and the name of the task
      val name = "no_partition_" + Math.abs(Random.nextInt)

      // create the sink topic
      Try {
        admin.createTopics(List(new NewTopic(name, 1, 1)).asJavaCollection).all().get(30, TimeUnit.SECONDS)
      }

      val taskDef = Source.fromInputStream(getClass.getResourceAsStream("/hive_sink_task1.json")).getLines().mkString("\n")
        .replace("{{TOPIC}}", name)
        .replace("{{TABLE}}", name)
        .replace("{{NAME}}", name)

      // first we start the kafka connect task by sending a POST to the kafka connect server
      val f = client.preparePost("http://localhost:8083/connectors")
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .setBody(taskDef)
        .execute()
      val resp = f.get(30, TimeUnit.SECONDS)
      println(resp.getResponseBody)
      resp.getStatusCode shouldBe 201

      // now we write in the records to kafka so they will be picked up by connect
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://127.0.0.1:9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
      val producer = new KafkaProducer[String, String](props)

      for (_ <- 1 to 1000) {
        producer.send(new ProducerRecord(name, JacksonSupport.mapper.writeValueAsString(person)))
        producer.flush()
      }

      producer.close(30, TimeUnit.SECONDS)

      // we now should have 1000 records in hive which we can test via jdbc
      eventually {
        val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "")
        val stmt = conn.createStatement
        val rs = stmt.executeQuery(s"select count(*) from $name")
        rs.next()
        rs.getLong(1) shouldBe 1000
        conn.close()
      }
    }
  }
}
