package io.lenses.streamreactor.connect.test

import _root_.io.lenses.streamreactor.connect.testcontainers.KafkaConnectContainer
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.testcontainers.KafkaVersions.ConfluentVersion
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, EitherValues}
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.{KafkaContainer, Network}
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.{Properties, UUID}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

object HttpSinkTest extends AsyncFlatSpec
    with AsyncIOSpec
    with Matchers
  with StreamReactorContainerPerSuite
    with LazyLogging
    with EitherValues
    with TableDrivenPropertyChecks {


  private lazy val container: S3Container = S3Container()
    .withNetwork(network)

  override val connectorModule: String = "http"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

}
