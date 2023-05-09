package io.lenses.streamreactor.connect.testcontainers.scalatest

import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient
import io.lenses.streamreactor.connect.testcontainers.{KafkaConnectContainer, SchemaRegistryContainer}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minute, Span}
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{KafkaContainer, Network}
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.{Properties, UUID}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

trait StreamReactorContainerPerSuite extends BeforeAndAfterAll with Eventually with LazyLogging { this: TestSuite =>

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(1, Minute))

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val confluentPlatformVersion: String = {
    val (vers, from) = sys.env.get("CONFLUENT_VERSION") match {
      case Some(value) => (value, "env")
      case None => ("7.3.1", "default")
    }
     log.info("Selected confluent version {} from {}", vers,from )
    vers
  }

  val network: Network = Network.SHARED

  def connectorModule: String
  def providedJars(): Seq[String] = Seq()

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
      providedJars            = providedJars(),
    ).withNetwork(network).withLogConsumer(new Slf4jLogConsumer(log))
  }

  // Override for different SchemaRegistryContainer configs
  def schemaRegistryContainer(): Option[SchemaRegistryContainer] =
    SchemaRegistryContainer(kafkaContainer = kafkaContainer)
      .withLogConsumer(new Slf4jLogConsumer(log))
      .withNetwork(network)
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

  private def connectPluginPath(): String = {
    val regex           = s".*$connectorModule.*.jar"
    Try{
      Files
        .find(artifactDir, 5, (p, _) => p.toFile.getName.matches(regex))
        .iterator()
        .asScala
        .next()
    } match {
      case Failure(exception) => fail(s"""Please run `sbt "project $connectorModule" assembly""", exception)
      case Success(nextFile) => nextFile.getParent.toString
    }
  }

  private def artifactDir = {
    val artDir = Option(sys.props("artifact.dir"))
    val userDir = sys.props("user.dir")
    logger.info("artDir: {} userDir: {}", artDir, userDir)

    Paths.get(artDir.getOrElse(userDir))
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
