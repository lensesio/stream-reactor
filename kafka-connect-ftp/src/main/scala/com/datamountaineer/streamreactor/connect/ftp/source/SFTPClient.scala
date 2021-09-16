package com.datamountaineer.streamreactor.connect.ftp.source

import com.jcraft.jsch.{ChannelSftp, JSch, Session}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.net.ftp.{FTPClient, FTPFile}

import java.io.OutputStream
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import scala.util.{Failure, Success, Try}

class SFTPClient extends FTPClient {

  var hostname: String = _
  var explicitPort: Int = _

  var maybeJschSession: Option[Session] = None
  var maybeChannelSftp: Option[ChannelSftp] = None

  override def retrieveFile(remote: String, local: OutputStream): Boolean = {
    maybeChannelSftp match {
      case Some(channelSftp) =>
        //TODO:Once the channel is close before, open again
        //        channelSftp.connect()
        channelSftp.get(remote, local)
        //        channelSftp.exit()
        true
      case None => false
    }
  }


  override def isConnected(): Boolean = {
    maybeChannelSftp.isDefined && maybeJschSession.get.isConnected
  }

  override def disconnect(): Unit = {
    if (maybeChannelSftp.isDefined) maybeChannelSftp.get.disconnect()
    if (maybeJschSession.isDefined) maybeJschSession.get.disconnect()
  }

  override def getReplyCode(): Int = {
    200
  }

  override def login(username: String, password: String): Boolean = {
    getChannel(username, password) match {
      case Success((session, channel)) =>
        maybeJschSession = Some(session)
        maybeChannelSftp = Some(channel)
      case Failure(exception) =>
        //TODO:Add me in the logs
        println(exception)
    }
    maybeChannelSftp.isDefined
  }

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


  override def listFiles(pathname: String): Array[FTPFile] = {


    //TODO:Control side-effects
    val channel = maybeChannelSftp.get
    if (!channel.isConnected) channel.connect()
    println(s"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ $pathname")
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


  private val remoteHost = "test.rebex.net"

  private def getChannel(username: String,
                         password: String): Try[(Session, ChannelSftp)] = {
    Try {
      val jsch = new JSch()
      //TODO:Pass as argument in case is mandatory
      //      jsch.setKnownHosts("/Users/john/.ssh/known_hosts")
      //TODO:Only for testing
      val config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");


      val jschSession: Session = jsch.getSession(username, remoteHost)
      jschSession.setPassword(password)
      //TODO:Only for testing
      jschSession.setConfig(config);
      jschSession.connect()
      (jschSession, jschSession.openChannel("sftp").asInstanceOf[ChannelSftp])
    }
  }

}

