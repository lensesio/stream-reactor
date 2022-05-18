package com.datamountaineer.streamreactor.connect.ftp

import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.apache.ftpserver.{FtpServer, FtpServerFactory}
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

import java.net.ServerSocket

class EmbeddedFtpServer extends StrictLogging {
  val username = "my-User_name7"
  val password = "=541%2@$;;'`"
  val host = "localhost"
  val port = getFreePort
  val rootDir = File.newTemporaryDirectory().path

  var server: FtpServer = null

  def start() = {
    val userManagerFactory = new PropertiesUserManagerFactory()
    val userManager = userManagerFactory.createUserManager()
    val user = new BaseUser()
    user.setName(username)
    user.setPassword(password)
    user.setHomeDirectory(rootDir.toString)
    userManager.save(user)
    val serverFactory = new FtpServerFactory()
    val listenerFactory = new ListenerFactory()
    listenerFactory.setPort(port)
    listenerFactory.setServerAddress(host)
    serverFactory.setUserManager(userManager)
    serverFactory.addListener("default", listenerFactory.createListener())
    server = serverFactory.createServer()
    server.start()
  }

  def stop() = server.stop()

  private def getFreePort: Int = {
    val socket = new ServerSocket(0)
    socket.setReuseAddress(true)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}
