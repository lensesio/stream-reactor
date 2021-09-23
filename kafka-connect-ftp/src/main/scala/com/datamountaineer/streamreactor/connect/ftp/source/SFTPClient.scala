package com.datamountaineer.streamreactor.connect.ftp.source

import com.datamountaineer.streamreactor.connect.ftp.source.SFTPClient.{Password, Username}
import com.jcraft.jsch.{ChannelSftp, JSch, Session}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.net.ftp.{FTPClient, FTPFile}

import java.io.OutputStream
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.{GregorianCalendar, Properties}
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

  private var lastReplyCode: Int = 500
  private var maybeConnectTimeout: Option[Int] = None
  private var maybeDataTimeout: Option[Int] = None
  private var maybeHostname: Option[String] = None
  private var maybeExplicitPort: Option[Int] = None
  private var maybeJschSession: Option[Session] = None
  private var maybeChannelSftp: Option[ChannelSftp] = None

  private val dateFormat = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss zzz uuuu")

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
    maybeJschSession.exists(_.isConnected)
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
    getSessionAndChannel(Username(username), Password(password))
      .map { case (session, channel) =>
        maybeJschSession = Some(session)
        maybeChannelSftp = Some(channel)
        logger.debug(s"SFTPClient Successful Session/Channel created by username $username.")
        lastReplyCode = 200
      }.recover {
      case e: Exception =>
        logger.error(s"SFTPClient error login username $username. Caused by ${ExceptionUtils.getStackTrace(e)}")
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
    maybeChannelSftp.fold {
      logger.error(s"SFTPClient Error no channel ready to obtain files from pathname $pathname.")
      Array[FTPFile]()
    } { channel =>
      if (!channel.isConnected) connectChannel(channel)
      logger.debug(s"SFTPClient obtaining remote files from $pathname")
      val ftpFiles = getFTPFiles(pathname, channel)
      logger.debug(s"SFTPClient ${ftpFiles.size} remote files obtained from $pathname")
      ftpFiles.toArray
    }
  }

  /**
    * Using the remote path of the file, and using [get] operator we're able to download the file and write
    * the content into the [OutputStream].
    */
  override def retrieveFile(remote: String, fileBody: OutputStream): Boolean = {
    maybeChannelSftp.fold {
      logger.debug(s"SFTPClient Error, channel not initiated in path $remote.")
      false
    } { channel =>
      channel.get(remote, fileBody)
      logger.debug(s"SFTPClient Successful retrieving files in path $remote.")
      true
    }
  }

  private def getFTPFiles(pathname: String, channel: ChannelSftp): List[FTPFile] = {
    (for {
      _ <- Try(channel.cd(pathname))
      ftpFiles <- fetchFiles(pathname, channel)
    } yield ftpFiles)
      .recover {
        case e: Exception =>
          logger.error(s"SFTPClient Error obtaining resources from pathname $pathname. Caused by ${ExceptionUtils.getStackTrace(e)}")
          List()
      }.get
  }

  private def connectChannel(channel: ChannelSftp): Unit = {
    maybeDataTimeout
      .fold(channel.connect())(dataTimeout => channel.connect(dataTimeout))
  }

  private def fetchFiles(pathname: String, channel: ChannelSftp): Try[List[FTPFile]] = {
    Try {
      channel.ls(pathname)
        .asScala
        .toList
        .map(file => transformToLsEntry(file))
        .filter(lsEntry => lsEntry.getFilename != "." && lsEntry.getFilename != "..")
        .map(lsEntry => createFtpFile(lsEntry))
    }
  }

  private def transformToLsEntry(file: Any): ChannelSftp#LsEntry = {
    file match {
      case lsEntry:ChannelSftp#LsEntry => lsEntry
      case unknown:Any => throw new ClassCastException(s"SFTPClient Error obtaining LsEntry. Unknown type $unknown")
    }
  }

  private def createFtpFile(lsEntry: ChannelSftp#LsEntry) = {
    val ftpFile: FTPFile = new FTPFile()
    ftpFile.setType(0)
    ftpFile.setName(lsEntry.getFilename)
    ftpFile.setSize(lsEntry.getAttrs.getSize)

    val date = ZonedDateTime.parse(lsEntry.getAttrs.getMtimeString, dateFormat)
    ftpFile.setTimestamp(GregorianCalendar.from(date))
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
    session =>
      session.openChannel("sftp") match {
        case channelSftp: ChannelSftp => Success(channelSftp)
        case unknown:Any => Failure(new ClassCastException(s"SFTPClient Error obtaining ChannelSftp. Unknown Channel type $unknown"))
      }
  }

  /**
    * Create and open a session in default port 22 or in a specific one using hostname, username and password
    */
  private val openSession: (Username, Password) => Try[Session] = {
    (username, password) =>
      for {
        hostname <- getHostname(username)
        session <- Try {
          val jsch = new JSch()
          val session = createSession(username, jsch, hostname)
          session.setPassword(password.value)
          setSessionConfig(session)
          connectSession(session)
          session
        }
      } yield session
  }

  private def setSessionConfig(session: Session): Unit = {
    val config = new Properties();
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config);
  }

  private def connectSession(session: Session): Unit = {
    maybeConnectTimeout
      .fold(session.connect()) {
        connectTimeout => session.connect(connectTimeout)
      }
  }

  private def createSession(username: Username, jsch: JSch, hostname: String): Session = {
    maybeExplicitPort
      .fold(jsch.getSession(username.value, hostname)) {
        explicitPort => jsch.getSession(username.value, hostname, explicitPort)
      }
  }

  private def getHostname(username: Username): Try[String] = {
    maybeHostname match {
      case Some(hostname) => Success(hostname)
      case None => Failure(new NoSuchElementException(s"Hostname not provided in transaction for Username $username"))
    }
  }
}

object SFTPClient {

  case class Username(value: String) extends AnyVal

  case class Password(value: String) extends AnyVal

}
