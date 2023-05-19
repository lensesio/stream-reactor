package com.datamountaineer.streamreactor.connect

import com.jayway.jsonpath.JsonPath
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.scalatest.fixtures.connect.withConnector
import io.lenses.streamreactor.connect.testcontainers.{ElasticsearchContainer, SchemaRegistryContainer}
import okhttp3.{OkHttpClient, Request}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using

class Elastic6Test extends AnyFlatSpec with StreamReactorContainerPerSuite with Matchers {

  lazy val container: ElasticsearchContainer = ElasticsearchContainer().withNetwork(network)

  override val schemaRegistryContainer: Option[SchemaRegistryContainer] = None

  override val connectorModule: String = "elastic6"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  behavior of "Elastic6 connector"

  it should "sink records" in {
    Using.resource(createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])) {
      producer =>
        withConnector("elastic-sink", sinkConfig(), 30L) {
          // Write records to topic
          val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100)
          producer.send(new ProducerRecord[String, Order]("orders", order)).get()
          producer.flush()

          val client = new OkHttpClient()
          val request = new Request.Builder().url(
            "http://" + container.hostNetwork.httpHostAddress + "/orders/_search/?q=OP-DAX-P-20150201",
          ).build

          eventually {
            val response = client.newCall(request).execute()
            val body     = response.body.string
            assert(JsonPath.read[Int](body, "$.hits.total") == 1)
          }

          val response = client.newCall(request).execute
          val body     = response.body.string
          JsonPath.read[Int](body, "$.hits.hits[0]._source.id") should be(1)
          JsonPath.read[String](body, "$.hits.hits[0]._source.product") should be("OP-DAX-P-20150201-95.7")
          JsonPath.read[Double](body, "$.hits.hits[0]._source.price") should be(94.2)
          JsonPath.read[Int](body, "$.hits.hits[0]._source.qty") should be(100)
        }
    }
  }

  private def sinkConfig(): ConnectorConfiguration = {
    val config = ConnectorConfiguration.create
    config.`with`("connector.class", "com.datamountaineer.streamreactor.connect.elastic6.ElasticSinkConnector")
    config.`with`("tasks.max", "1")
    config.`with`("topics", "orders")
    config.`with`("connect.elastic.protocol", "http")
    config.`with`("connect.elastic.hosts", container.networkAlias)
    config.`with`("connect.elastic.port", Integer.valueOf(container.port))
    config.`with`("connect.elastic.cluster.name", "elasticsearch")
    config.`with`("connect.elastic.kcql", "INSERT INTO orders SELECT * FROM orders")
    config.`with`("connect.progress.enabled", "true")
  }
}
