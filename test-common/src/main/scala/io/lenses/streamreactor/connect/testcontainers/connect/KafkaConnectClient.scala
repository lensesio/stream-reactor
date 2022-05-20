package io.lenses.streamreactor.connect.testcontainers.connect

import com.typesafe.scalalogging.StrictLogging
import io.debezium.testing.testcontainers.Connector
import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.lenses.streamreactor.connect.testcontainers.KafkaConnectContainer
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.ConnectorStatus
import org.apache.http.HttpHeaders
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.concurrent.Eventually
import org.testcontainers.shaded.org.awaitility.Awaitility.await

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class KafkaConnectClient(kafkaConnectContainer: KafkaConnectContainer) extends StrictLogging with Eventually {

  implicit val formats: DefaultFormats.type = DefaultFormats

  val httpClient: HttpClient = {
    val acceptHeader  = new BasicHeader(HttpHeaders.ACCEPT, "application/json")
    val contentHeader = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    HttpClientBuilder.create
      .setDefaultHeaders(List(acceptHeader, contentHeader).asJava)
      .build()
  }

  def registerConnector(
    name:                   String,
    connectorConfiguration: ConnectorConfiguration,
    timeoutSeconds:         Long = 10L,
  ): Unit = {
    val connector = Connector.from(name, connectorConfiguration)
    val httpPost  = new HttpPost(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/connectors")
    val entity    = new StringEntity(connector.toJson)
    httpPost.setEntity(entity)
    val response = httpClient.execute(httpPost)
    checkRequestSuccessful(response)
    EntityUtils.consume(response.getEntity)
    await.atMost(timeoutSeconds, TimeUnit.SECONDS).until(() => this.isConnectorConfigured(connector.getName))
  }

  def deleteConnector(connectorName: String, timeoutSeconds: Long = 10L): Unit = {
    val httpDelete =
      new HttpDelete(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/connectors/$connectorName")
    val response = httpClient.execute(httpDelete)
    checkRequestSuccessful(response)
    EntityUtils.consume(response.getEntity)
    await.atMost(timeoutSeconds, TimeUnit.SECONDS).until(() => !this.isConnectorConfigured(connectorName))
  }

  def getConnectorStatus(connectorName: String): ConnectorStatus = {
    val httpGet  = new HttpGet(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/connectors/$connectorName/status")
    val response = httpClient.execute(httpGet)
    checkRequestSuccessful(response)
    val strResponse = EntityUtils.toString(response.getEntity)
    parse(strResponse).extract[ConnectorStatus]
  }

  def isConnectorConfigured(connectorName: String): Boolean = {
    val httpGet  = new HttpGet(s"${kafkaConnectContainer.hostNetwork.restEndpointUrl}/connectors/$connectorName")
    val response = httpClient.execute(httpGet)
    EntityUtils.consume(response.getEntity)
    response.getStatusLine.getStatusCode == 200
  }

  def waitConnectorInRunningState(connectorName: String, timeoutSeconds: Long = 10L): Unit =
    await.atMost(timeoutSeconds, TimeUnit.SECONDS)
      .until { () =>
        try {
          getConnectorStatus(connectorName).connector.state.equals("RUNNING")
        } catch {
          case _: Throwable => false
        }
      }

  def checkRequestSuccessful(response: HttpResponse): Unit =
    if (!isSuccess(response.getStatusLine.getStatusCode)) {
      throw new IllegalStateException(s"Http request failed with response: ${EntityUtils.toString(response.getEntity)}")
    }

  def isSuccess(code: Int): Boolean = code / 100 == 2
}

object KafkaConnectClient {
  case class ConnectorStatus(name: String, connector: Connector, tasks: Seq[Tasks], `type`: String)
  case class Connector(state: String, worker_id: String)
  case class Tasks(id: Int, state: String, worker_id: String, trace: Option[String])
}
