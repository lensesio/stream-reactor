package com.landoop.streamreactor.hive.it

import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.asynchttpclient.Dsl
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class HiveNoPartitionTest extends WordSpec with Matchers with TestData {
  Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver")

  private val client = Dsl.asyncHttpClient(Dsl.config())

  private val admin = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    AdminClient.create(props)
  }

  "Hive" should {
    "write records" in {

      // create the sink topic
      admin.createTopics(List(new NewTopic("hive_sink_topic1", 1, 1)).asJavaCollection).all().get(30, TimeUnit.SECONDS)

      // first we start the kafka connect task by sending a POST to the kafka connect server
      val f = client.preparePost("http://localhost:8083").setBody(getClass.getResourceAsStream("/hive_sink_task1.json")).execute()
      val resp = f.get(30, TimeUnit.SECONDS)
      println(resp.getResponseBody)

      // now we write in 1 million records
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
      val producer = new KafkaProducer[String, String](props)

      for (_ <- 1 to 1000000) {
        producer.send(new ProducerRecord("hive_sink_topic1", JacksonSupport.mapper.writeValueAsString(person)))
      }

      producer.close(30, TimeUnit.SECONDS)

      // we now should have 1m records in hive which we can test via jdbc
      val conn = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "")
      val stmt = conn.createStatement
      val rs = stmt.executeQuery("select count(*) from hive_sink_table1")
      rs.next()
      rs.getLong(1) shouldBe 1000000
      conn.close()
    }
  }
}
