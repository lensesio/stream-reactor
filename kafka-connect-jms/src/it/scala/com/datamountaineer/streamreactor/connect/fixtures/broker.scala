package com.datamountaineer.streamreactor.connect.fixtures

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService

import java.io.File
import java.net.ServerSocket
import javax.jms.Connection

object broker {

  def testWithBrokerOnPort(test: (Connection, String) => Unit): Unit = testWithBrokerOnPort()(test)

  def testWithBrokerOnPort(port: Int = getFreePort)(test: (Connection, String) => Unit): Unit = {
    val _ = testWithBroker(port, None) { brokerUrl =>
      val connectionFactory = new ActiveMQConnectionFactory()
      connectionFactory.setBrokerURL(brokerUrl)
      val conn = connectionFactory.createConnection()
      conn.start()
      test(conn, brokerUrl)
    }
  }

  def testWithBroker(port: Int = getFreePort, clientID: Option[String])(test: String => Unit): BrokerService = {
    val broker = new BrokerService()
    broker.setPersistent(false)
    broker.setUseJmx(false)
    broker.setDeleteAllMessagesOnStartup(true)
    val brokerUrl = s"tcp://localhost:$port${clientID.fold("")(id => s"?jms.clientID=$id")}"
    broker.addConnector(brokerUrl)
    broker.setUseShutdownHook(false)
    val property = "java.io.tmpdir"
    val tempDir  = System.getProperty(property)
    broker.setDataDirectoryFile(new File(tempDir))
    broker.setTmpDataDirectory(new File(tempDir))
    broker.start()
    test(brokerUrl)
    broker
  }

  private def getFreePort: Int = {
    val socket = new ServerSocket(0)
    socket.setReuseAddress(true)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}
