package com.datamountaineer.streamreactor.connect.ftp.source

import com.datamountaineer.streamreactor.connect.ftp.source.SFTPClient.{Password, Username}
import com.jcraft.jsch.{ChannelSftp, JSch, Session}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.net.ftp.{FTPClient, FTPFile}

import java.io.OutputStream
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Properties}
import scala.util.{Failure, Success, Try}

class SFTPClient extends FTPClient with StrictLogging {

  var hostname: String = _
  var explicitPort: Int = _
  var lastReplyCode: Int = 200
  var maybeJschSession: Option[Session] = None
  var maybeChannelSftp: Option[ChannelSftp] = None

  override def retrieveFile(remote: String, local: OutputStream): Boolean = {
    maybeChannelSftp match {
      case Some(channelSftp) =>
        channelSftp.get(remote, local)
        logger.debug(s"SFTPClient Successful retrieving files in path $remote.")
        true
      case None =>
        logger.debug(s"SFTPClient Error, channel not initiated in path $remote.")
        false
    }
  }

  override def login(username: String, password: String): Boolean = {
    getSessionAndChannel(Username(username), Password(password)) match {
      case Success((session, channel)) =>
        maybeJschSession = Some(session)
        maybeChannelSftp = Some(channel)
        logger.debug(s"SFTPClient Successful Channel created by username $username.")
      case Failure(exception) =>
        logger.error(s"SFTPClient error login username $username. Caused by ${ExceptionUtils.getStackTrace(exception)}")
    }
    maybeJschSession.isDefined && maybeChannelSftp.isDefined
  }

  override def listFiles(pathname: String): Array[FTPFile] = {
    //TODO:Control side-effects
    val channel = maybeChannelSftp.get
    if (!channel.isConnected) channel.connect()
    logger.debug(s"SFTPClient obtaining remote files from $pathname")
    //TODO:Mutable Array uggggg fix me!
    var ftpFiles = List[FTPFile]()
    Try(channel.cd(pathname)) match {
      case Success(_) =>
        val files: util.Vector[_] = channel.ls(pathname)
        files.forEach(file => {
          val entry = file.asInstanceOf[ChannelSftp#LsEntry]
          if (entry.getFilename != "." && entry.getFilename != "..") {
            val ftpFile: FTPFile = new FTPFile()
            ftpFile.setType(0)
            ftpFile.setName(entry.getFilename)
            ftpFile.setSize(entry.getAttrs.getSize)

            val dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu")
            val calendar = Calendar.getInstance()
            calendar.setTime(dateFormat.parse(entry.getAttrs.getMtimeString))
            ftpFile.setTimestamp(calendar)

            ftpFiles = ftpFiles ++ List(ftpFile)
          }
        })
      case Failure(t) =>
        //TODO:Log me
        println(s"Error obtaining resources from pathname $pathname. Caused by ${ExceptionUtils.getStackTrace(t)}")
    }
    ftpFiles.toArray
    //TODO:Close channel
  }

  override def isConnected(): Boolean = {
    maybeChannelSftp.isDefined && maybeJschSession.get.isConnected
  }

  override def disconnect(): Unit = {
    if (maybeChannelSftp.isDefined) maybeChannelSftp.get.disconnect()
    if (maybeJschSession.isDefined) maybeJschSession.get.disconnect()
  }

  override def getReplyCode(): Int = lastReplyCode

  override def connect(hostname: String, explicitPort: Int): Unit = {
    this.hostname = hostname
    this.explicitPort = explicitPort
  }

  override def connect(hostname: String): Unit = {
    this.hostname = hostname
  }

  override def setFileType(fileType: Int): Boolean = {
    true
  }

  private val remoteHost = "test.rebex.net"

  private def getSessionAndChannel(username: Username,
                                   password: Password): Try[(Session, ChannelSftp)] = {
    Try {
      val session: Session = createSession(username, password)
      val channel: ChannelSftp = createChannel(session)
      (session, channel)
    }
  }

  private val createChannel: Session => ChannelSftp = {
    session => session.openChannel("sftp").asInstanceOf[ChannelSftp]
  }

  private val createSession: (Username, Password) => Session = {
    (username, password) =>
      //TODO:Pass as argument in case is mandatory
      //      jsch.setKnownHosts("/Users/john/.ssh/known_hosts")
      //TODO:Only for testing
      val config = new Properties();
      config.put("StrictHostKeyChecking", "no")
      val jsch = new JSch()
      val jschSession: Session = jsch.getSession(username.value, remoteHost)
      jschSession.setPassword(password.value)
      //TODO:Only for testing
      jschSession.setConfig(config);
      jschSession.connect()
      jschSession
  }
}

object SFTPClient {

  case class Username(value: String) extends AnyVal

  case class Password(value: String) extends AnyVal


}
