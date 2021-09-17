package com.datamountaineer.streamreactor.connect.ftp.source

import com.datamountaineer.streamreactor.connect.ftp.source.SFTPClient.{Password, Username}
import com.jcraft.jsch.{ChannelSftp, JSch, Session}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.net.ftp.{FTPClient, FTPFile}

import java.io.OutputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class SFTPClient extends FTPClient with StrictLogging {

  var hostname: String = _
  var maybeExplicitPort: Option[Int] = None
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
    maybeChannelSftp match {
      case Some(channel) =>
        if (!channel.isConnected) channel.connect()
        logger.debug(s"SFTPClient obtaining remote files from $pathname")
        val ftpFiles: List[FTPFile] = Try(channel.cd(pathname)) match {
          case Success(_) => fetchFiles(pathname, channel)
          case Failure(t) =>
            logger.error(s"SFTPClient Error obtaining resources from pathname $pathname. Caused by ${ExceptionUtils.getStackTrace(t)}")
            List[FTPFile]()
        }
        logger.debug(s"SFTPClient ${ftpFiles.size} remote files obtained from $pathname")
        ftpFiles.toArray
      //TODO:Close channel
      case None =>
        logger.error(s"SFTPClient Error no channel ready to obtain files from pathname $pathname.")
        Array()
    }
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
    this.maybeExplicitPort = Some(explicitPort)
  }

  override def connect(hostname: String): Unit = {
    this.hostname = hostname
  }

  override def setFileType(fileType: Int): Boolean = {
    true
  }

  private def fetchFiles(pathname: String, channel: ChannelSftp): List[FTPFile] = {
    channel.ls(pathname)
      .asScala
      .toList
      .map(file => file.asInstanceOf[ChannelSftp#LsEntry])
      .filter(lsEntry => lsEntry.getFilename != "." && lsEntry.getFilename != "..")
      .map(lsEntry => createFtpFile(lsEntry))
  }

  private def createFtpFile(lsEntry: ChannelSftp#LsEntry) = {
    val ftpFile: FTPFile = new FTPFile()
    ftpFile.setType(0)
    ftpFile.setName(lsEntry.getFilename)
    ftpFile.setSize(lsEntry.getAttrs.getSize)

    val dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz uuuu")
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(lsEntry.getAttrs.getMtimeString))
    ftpFile.setTimestamp(calendar)
    ftpFile
  }

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
      val jschSession: Session = maybeExplicitPort match {
        case Some(explicitPort) => jsch.getSession(username.value, hostname, explicitPort)
        case None => jsch.getSession(username.value, hostname)
      }
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
