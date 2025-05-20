/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.testcontainers.scalatest

import cats.effect.IO
import cats.effect.Resource
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.lenses.streamreactor.connect.testcontainers.KafkaVersions.ConfluentVersion
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient
import io.lenses.streamreactor.connect.testcontainers.KafkaConnectContainer
import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Minute
import org.scalatest.time.Span
import org.scalatest.AsyncTestSuite
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.util.Properties
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait StreamReactorContainerPerSuite extends BeforeAndAfterAll with Eventually with LazyLogging {
  this: AsyncTestSuite =>

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(1, Minute))

  val network: Network = Network.SHARED

  def connectorModule: String
  def providedJars(): Seq[String] = Seq()

  lazy val kafkaContainer: KafkaContainer =
    new KafkaContainer(DockerImageName.parse(s"confluentinc/cp-kafka").withTag(ConfluentVersion))
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withLogConsumer(new Slf4jLogConsumer(logger.underlying))

  lazy val kafkaConnectContainer: KafkaConnectContainer = {
    KafkaConnectContainer(
      kafkaContainer          = kafkaContainer,
      schemaRegistryContainer = schemaRegistryContainer,
      connectPluginPath       = Some(connectPluginPath()),
      providedJars            = providedJars(),
    ).withNetwork(network).withLogConsumer(new Slf4jLogConsumer(logger.underlying))
  }

  // Override for different SchemaRegistryContainer configs
  val schemaRegistryContainer: Option[SchemaRegistryContainer] =
    SchemaRegistryContainer(kafkaContainer = kafkaContainer)
      .withLogConsumer(new Slf4jLogConsumer(logger.underlying))
      .withNetwork(network)
      .some

  implicit lazy val kafkaConnectClient: KafkaConnectClient = new KafkaConnectClient(kafkaConnectContainer)

  override def beforeAll(): Unit = {
    kafkaContainer.start()
    schemaRegistryContainer.foreach(_.start())
    kafkaConnectContainer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally {
      kafkaConnectContainer.stop()
      schemaRegistryContainer.foreach(_.stop())
      kafkaContainer.stop()
    }

  private def connectPluginPath(): String = {
    val regex = s".*$connectorModule.*.jar"
    Try {
      Files
        .find(artifactDir, 5, (p, _) => p.toFile.getName.matches(regex))
        .iterator()
        .asScala
        .next()
    } match {
      case Failure(exception) => fail(s"""Please run `sbt "project $connectorModule" assembly""", exception)
      case Success(nextFile)  => nextFile.getParent.toString
    }
  }

  private def artifactDir = {
    val artDir  = Option(sys.props("artifact.dir"))
    val userDir = sys.props("user.dir")
    logger.info("artDir: {} userDir: {}", artDir, userDir)

    Paths.get(artDir.getOrElse(userDir))
  }

  def createProducer[K, V](keySer: Class[_], valueSer: Class[_]): Resource[IO, KafkaProducer[K, V]] =
    Resource.fromAutoCloseable {
      IO {
        val props = new Properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
        props.put(ProducerConfig.ACKS_CONFIG, "all")
        props.put(ProducerConfig.RETRIES_CONFIG, 0)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer)
        schemaRegistryContainer.foreach(s => props.put(SCHEMA_REGISTRY_URL_CONFIG, s.hostNetwork.schemaRegistryUrl))
        new KafkaProducer[K, V](props)
      }
    }

  def createConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "cg-" + UUID.randomUUID())
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    new KafkaConsumer[String, String](props, new StringDeserializer(), new StringDeserializer())
  }

  /**
    * Drain a kafka topic.
    *
    * @param consumer the kafka consumer
    * @param expectedRecordCount the expected record count
    * @tparam K the key type
    * @tparam V the value type
    * @return the records
    */
  def drain[K, V](consumer: KafkaConsumer[K, V], expectedRecordCount: Int): List[ConsumerRecord[K, V]] = {
    val allRecords = ListBuffer[ConsumerRecord[K, V]]()
    eventually {
      consumer.poll(Duration.ofMillis(50))
        .iterator()
        .forEachRemaining(allRecords.addOne)
      assert(allRecords.size == expectedRecordCount)
    }
    allRecords.toList
  }
}
