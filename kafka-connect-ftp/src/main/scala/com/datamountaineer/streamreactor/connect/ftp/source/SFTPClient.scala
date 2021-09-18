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

/**
  * Implementation for Secure File Transfer Protocol, to allow Source a SFTP server
  * and continue using the rest of implementation of the Connector to send to Kafka topic.
  *
  * This class use the [Adapter] pattern to adapt the contract of [FTPClient] invoked from the
  * core library, to the [JsCh] implementation that we use internally.
  */
class SFTPClient extends FTPClient with StrictLogging {

  var lastReplyCode: Int = 500
  var maybeConnectTimeout: Option[Int] = None
  var maybeDataTimeout: Option[Int] = None
  var maybeHostname: Option[String] = None
  var maybeExplicitPort: Option[Int] = None
  var maybeJschSession: Option[Session] = None
  var maybeChannelSftp: Option[ChannelSftp] = None

  /**
    * We ensure that not only the session with the server is close, but also the channel that it might be open from a previous
    * transaction.
    */
  override def disconnect(): Unit = {
    maybeJschSession.foreach(session => session.disconnect())
    maybeChannelSftp.foreach(channel => channel.disconnect())
  }

  /**
    * We just check the session with the SFTP server is open
    */
  override def isConnected(): Boolean = {
    maybeJschSession.isDefined && maybeJschSession.get.isConnected
  }

  /**
    * Max Timeout in Ms to open a session with SFTP Server
    */
  override def setConnectTimeout(timeoutMs: Int) {
    maybeConnectTimeout = Some(timeoutMs)
  }

  /**
    * Max Timeout in Ms to connect a channel with SFTP Server
    */
  override def setDataTimeout(timeoutMs: Int) {
    maybeDataTimeout = Some(timeoutMs)
  }

  /**
    * Using JsCh library, the only thing we can do in this moment it's to keep the information
    * passed to be used later in subsequent steps
    */
  override def connect(hostname: String, explicitPort: Int): Unit = {
    this.maybeHostname = Some(hostname)
    this.maybeExplicitPort = Some(explicitPort)
    this.lastReplyCode = 200
  }

  /**
    * Using JsCh library, the only thing we can do in this moment it's to keep the information
    * passed to be used later in subsequent steps
    */
  override def connect(hostname: String): Unit = {
    this.maybeHostname = Some(hostname)
    this.lastReplyCode = 200
  }

  /**
    * Code number to keep the state of the Connector [200 => OK, 500 => ERROR]
    */
  override def getReplyCode(): Int = lastReplyCode

  /**
    * Using username and password together with the previous info passed(hostname, explicitPort?)
    * we're able to open an connect a session with the SFTP server, and create a Channel to
    * be used later in subsequent steps to get folder info, or download files.
    */
  override def login(username: String, password: String): Boolean = {
    getSessionAndChannel(Username(username), Password(password)) match {
      case Success((session, channel)) =>
        maybeJschSession = Some(session)
        maybeChannelSftp = Some(channel)
        logger.debug(s"SFTPClient Successful Session/Channel created by username $username.")
        lastReplyCode = 200
      case Failure(exception) =>
        logger.error(s"SFTPClient error login username $username. Caused by ${ExceptionUtils.getStackTrace(exception)}")
        lastReplyCode = 500
    }
    maybeJschSession.isDefined && maybeChannelSftp.isDefined
  }

  /**
    * Not used in this implementation of [JsCh]
    */
  override def setFileType(fileType: Int): Boolean = {
    true
  }

  /**
    * Connect to SFTP server to obtain files information (name, size, last modify)
    * and it returns a Array[FTPFile] with all directory file info.
    */
  override def listFiles(pathname: String): Array[FTPFile] = {
    maybeChannelSftp match {
      case Some(channel) =>
        if (!channel.isConnected) connectChannel(channel)
        logger.debug(s"SFTPClient obtaining remote files from $pathname")
        val ftpFiles = getFTPFiles(pathname, channel)
        logger.debug(s"SFTPClient ${ftpFiles.size} remote files obtained from $pathname")
        ftpFiles.toArray
      case None =>
        logger.error(s"SFTPClient Error no channel ready to obtain files from pathname $pathname.")
        Array()
    }
  }

  /**
    * Using the remote path of the file, and using [get] operator we're able to download the file and write
    * the content into the [OutputStream].
    */
  override def retrieveFile(remote: String, fileBody: OutputStream): Boolean = {
    maybeChannelSftp match {
      case Some(channel) =>
        channel.get(remote, fileBody)
        logger.debug(s"SFTPClient Successful retrieving files in path $remote.")
        true
      case None =>
        logger.debug(s"SFTPClient Error, channel not initiated in path $remote.")
        false
    }
  }

  private def getFTPFiles(pathname: String, channel: ChannelSftp): List[FTPFile] = {
    Try(channel.cd(pathname)) match {
      case Success(_) => fetchFiles(pathname, channel)
      case Failure(t) =>
        logger.error(s"SFTPClient Error obtaining resources from pathname $pathname. Caused by ${ExceptionUtils.getStackTrace(t)}")
        List[FTPFile]()
    }
  }

  private def connectChannel(channel: ChannelSftp): Unit = {
    maybeDataTimeout match {
      case Some(dataTimeout) => channel.connect(dataTimeout)
      case None => channel.connect()
    }
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
    for {
      session <- openSession(username, password)
      channel <- createChannel(session)
    } yield (session, channel)
  }

  /**
    * Open a channel with protocol [sftp].
    */
  private val createChannel: Session => Try[ChannelSftp] = {
    session => Try(session.openChannel("sftp").asInstanceOf[ChannelSftp])
  }

  /**
    * Create and open a session in default port 22 or in a specific one using hostname, username and password
    */
  private val openSession: (Username, Password) => Try[Session] = {
    (username, password) =>
      Try {
        val jsch = new JSch()
        val hostname = getHostname(username)
        val session = createSession(username, jsch, hostname)
        session.setPassword(password.value)
        setSessionConfig(session)
        connectSession(session)
        session
      }
  }

  private def setSessionConfig(session: Session): Unit = {
    val config = new Properties();
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config);
  }

  private def connectSession(session: Session): Unit = {
    maybeConnectTimeout match {
      case Some(connectTimeout) => session.connect(connectTimeout)
      case None => session.connect()
    }
  }

  private def createSession(username: Username, jsch: JSch, hostname: String): Session = {
    maybeExplicitPort match {
      case Some(explicitPort) => jsch.getSession(username.value, hostname, explicitPort)
      case None => jsch.getSession(username.value, hostname)
    }
  }

  private def getHostname(username: Username): String = {
    maybeHostname match {
      case Some(hostname) => hostname
      case None => throw new NoSuchElementException(s"Hostname not provided in transaction for Username $username")
    }
  }
}

object SFTPClient {

  case class Username(value: String) extends AnyVal

  case class Password(value: String) extends AnyVal

}
