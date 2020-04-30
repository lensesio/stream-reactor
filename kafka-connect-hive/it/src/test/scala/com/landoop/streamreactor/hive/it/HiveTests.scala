package com.landoop.streamreactor.hive.it

import java.sql.{Connection, DriverManager}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hive.jdbc.HiveDriver
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.asynchttpclient.Dsl
import org.scalatest.Matchers

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Random, Try}

trait HiveTests extends Matchers {
  new HiveDriver()

  private val http = Dsl.asyncHttpClient(Dsl.config())

  protected val admin: AdminClient = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://127.0.0.1:9092")
    AdminClient.create(props)
  }

  protected implicit val fs: FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:8020")
    FileSystem.get(conf)
  }

  protected implicit val metastore: HiveMetaStoreClient = {
    val hiveConf: HiveConf = new HiveConf()
    hiveConf.set("hive.metastore", "thrift")
    hiveConf.set("hive.metastore.uris", "thrift://localhost:9083")
    new HiveMetaStoreClient(hiveConf)
  }

  protected def createTopic(): String = {
    val name = "no_partition_" + Math.abs(Random.nextInt)
    Try {
      admin.createTopics(List(new NewTopic(name, 1, 1.toShort)).asJavaCollection).all().get(30, TimeUnit.SECONDS)
      name
    }.getOrElse(sys.error(s"Could not create topic $name"))
  }

  protected def postTask(taskDef: String): Unit = {
    val f = http.preparePost("http://localhost:8083/connectors")
      .addHeader("Accept", "application/json")
      .addHeader("Content-Type", "application/json")
      .setBody(taskDef)
      .execute()
    val resp = f.get(30, TimeUnit.SECONDS)
    println(resp.getResponseBody)
    resp.getStatusCode shouldBe 201
  }

  protected def stopTask(name: String): Unit = {
    val f = http.prepareDelete(s"http://localhost:8083/connectors/$name").execute()
    val resp = f.get(30, TimeUnit.SECONDS)
    println(resp.getResponseBody)
    resp.getStatusCode shouldBe 204
  }

  protected def stringStringProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://127.0.0.1:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    new KafkaProducer[String, String](props)
  }

  protected def stringStringConsumer(offset: String = "latest"): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://127.0.0.1:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + Math.abs(Random.nextInt))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset)
    new KafkaConsumer[String, String](props)
  }

  protected def writeRecords[T](producer: KafkaProducer[String, T], topic: String, create: => T, count: Long): Unit = {
    for (k <- 1 to count.toInt) {
      producer.send(new ProducerRecord(topic, create))
      if (k % 10000 == 0) {
        println(s"Produced $k records")
        producer.flush()
      }
    }
    producer.flush()
  }

  protected def readRecords[T](consumer: KafkaConsumer[String, T], topic: String, len: Long, unit: TimeUnit): Seq[ConsumerRecord[String, T]] = {
    consumer.poll(Duration.ofSeconds(len)).iterator().asScala.toList
  }

  protected def withConn(fn: Connection => Unit): Unit = {
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "")
    try {
      fn(conn)
    } catch {
      case NonFatal(e) =>
        println(e.getMessage)
        throw e
    } finally {
      conn.close()
    }
  }
}


