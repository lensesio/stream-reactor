package io.lenses.streamreactor.connect.testcontainers.scalatest

import cats.implicits.catsSyntaxOptionId
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.TestSuite
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.stream.Collectors
import java.util.Properties
import java.util.UUID
import scala.collection.mutable.ListBuffer

trait StreamReactorContainerPerSuite extends BeforeAndAfterAll with Eventually { this: TestSuite =>

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(1, Minute))

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val confluentPlatformVersion: String = sys.env.getOrElse("CONFLUENT_VERSION", "6.1.0")

  val network: Network = Network.SHARED

  def connectorModule: String

  lazy val schemaRegistryInstance: Option[SchemaRegistryContainer] = schemaRegistryContainer()

  lazy val kafkaContainer: KafkaContainer =
    new KafkaContainer(DockerImageName.parse(s"confluentinc/cp-kafka:$confluentPlatformVersion"))
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withLogConsumer(new Slf4jLogConsumer(log))

  lazy val kafkaConnectContainer: KafkaConnectContainer = {
    KafkaConnectContainer(
      kafkaContainer          = kafkaContainer,
      schemaRegistryContainer = schemaRegistryContainer(),
      connectPluginPath       = Some(connectPluginPath()),
    ).withLogConsumer(new Slf4jLogConsumer(log))
  }

  // Override for different SchemaRegistryContainer configs
  def schemaRegistryContainer(): Option[SchemaRegistryContainer] =
    SchemaRegistryContainer(kafkaContainer = kafkaContainer)
      .withLogConsumer(new Slf4jLogConsumer(log))
      .some

  implicit lazy val kafkaConnectClient: KafkaConnectClient = new KafkaConnectClient(kafkaConnectContainer)

  override def beforeAll(): Unit = {
    kafkaContainer.start()
    schemaRegistryInstance.foreach(_.start())
    kafkaConnectContainer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally {
      kafkaConnectContainer.stop()
      schemaRegistryInstance.foreach(_.stop())
      kafkaContainer.stop()
    }

  def connectPluginPath(): String = {
    val directorySuffix = sys.env("KAFKA_VERSION_DIRECTORY_SUFFIX")
    val regex           = s".*$connectorModule$directorySuffix.*.jar"
    val files: java.util.List[Path] = Files.find(
      Paths.get(String.join(File.separator, sys.props("user.dir"), "kafka-connect-" + connectorModule, "target")),
      3,
      (p, _) => p.toFile.getName.matches(regex),
    ).collect(Collectors.toList())
    if (files.isEmpty)
      throw new RuntimeException(s"""Please run `sbt "project $connectorModule$directorySuffix" assembly""")
    files.get(0).getParent.toString
  }

  def createProducer[K, V](keySer: Class[_], valueSer: Class[_]): KafkaProducer[K, V] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, 0)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer)
    schemaRegistryInstance.foreach(s => props.put(SCHEMA_REGISTRY_URL_CONFIG, s.hostNetwork.schemaRegistryUrl))
    new KafkaProducer[K, V](props)
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
